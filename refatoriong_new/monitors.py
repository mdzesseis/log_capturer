"""
monitors.py - Monitoramento de containers Docker e arquivos.
- ContainerMonitor: escuta eventos Docker, inicia/paralisa monitoramento, gerencia concorrência.
- FileMonitor: monitora arquivos, salva posição, envia logs para dispatcher.
- Fluxo robusto, com health check, persistência de posição e isolamento de tarefas.
"""

import os
import time
import json
import aiofiles
import asyncio
import hashlib
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from .metrics import metrics
from .task_manager import task_manager, TaskPriority
from .config import POSITIONS_DIR, POSITION_SAVE_INTERVAL, DOCKER_LABEL_FILTER_ENABLED, DOCKER_LABEL_FILTER, TASK_HEALTH_CHECK_INTERVAL, DOCKER_CONNECTION_TIMEOUT, DOCKER_CONNECTION_REQUIRED
from .config import DOCKER_API_RATE_LIMIT, DOCKER_API_BURST_SIZE, DOCKER_CONNECTION_POOL_SIZE, DOCKER_CONNECTION_POOL_MAX_SIZE, DOCKER_METADATA_CACHE_TTL
from .robustness import StructuredLogger
from .optimization import unified_cleanup_manager  # <--- registrar limpeza de positions
# NOVO: flags de hot-path logging
from .config import HOTPATH_DEBUG_ENABLED, HOTPATH_DEBUG_SAMPLE_N

# NOVO: import seguro do aiodocker e flag de disponibilidade
try:
    import aiodocker  # type: ignore
    DOCKER_AVAILABLE = True
except Exception:
    aiodocker = None  # type: ignore
    DOCKER_AVAILABLE = False

async def check_docker_connection(timeout: int = DOCKER_CONNECTION_TIMEOUT) -> bool:
    if not DOCKER_AVAILABLE:
        return False
    try:
        docker = aiodocker.Docker()
        await asyncio.wait_for(docker.system.info(), timeout=timeout)
        await docker.close()
        return True
    except Exception:
        return False

# Helper de escrita atômica compartilhado (pos/since)
async def _atomic_write(path: Path, data: str):
    tmp_path = Path(f"{str(path)}.tmp")
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(tmp_path, 'w') as f:
        await f.write(data)
        await f.flush()
    os.replace(str(tmp_path), str(path))

class FileMonitor:
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        self.monitored_files: Dict[str, Dict[str, Any]] = {}
        self.positions_dir = POSITIONS_DIR
        self.logger = StructuredLogger("file_monitor")
        self.logger.debug("FileMonitor inicializado", positions_dir=str(self.positions_dir))
        # Registrar TTL/limpeza para arquivos de posição (.pos) e de containers (.since)
        try:
            unified_cleanup_manager.register_cleanup_target(self.positions_dir, max_size_mb=64, patterns=["*.pos", "*.since", "*.tmp"])
        except Exception:
            pass
    def _get_position_file_path(self, log_path: Path) -> Path:
        self.logger.debug("Gerando caminho do arquivo de posição", log_path=str(log_path))
        path_hash = hashlib.md5(str(log_path.resolve()).encode()).hexdigest()
        return self.positions_dir / f"{path_hash}.pos"
    async def _read_position(self, pos_file: Path) -> int:
        self.logger.debug("Lendo posição do arquivo", pos_file=str(pos_file))
        if not pos_file.exists():
            return 0
        try:
            async with aiofiles.open(pos_file, 'r') as f:
                content = await f.read()
                position = int(content.strip())
                metrics.FILE_POSITION_BYTES.labels(filepath=str(pos_file)).set(position)
                return position
        except Exception:
            metrics.POSITION_FILE_ERRORS.labels(filepath=str(pos_file), operation='read').inc()
            return 0
    async def _save_position(self, pos_file: Path, position: int):
        self.logger.debug("Salvando posição do arquivo", pos_file=str(pos_file), position=position)
        try:
            # Escrita atômica: grava em .tmp e faz replace
            await _atomic_write(pos_file, str(position))
            metrics.FILE_POSITION_BYTES.labels(filepath=str(pos_file)).set(position)
            metrics.POSITION_FILE_UPDATES.labels(filepath=str(pos_file)).inc()
        except Exception:
            metrics.POSITION_FILE_ERRORS.labels(filepath=str(pos_file), operation='write').inc()
    async def add_temporary_file(self, path: Path, labels: dict) -> str:
        self.logger.debug("Adicionando arquivo para monitoramento", path=str(path), labels=labels)
        str_path = str(path.resolve())
        if str_path in self.monitored_files:
            return "file_already_monitored"
        task_name = f"file_{hashlib.md5(str_path.encode()).hexdigest()[:12]}"
        # REGISTRA E INICIA A TAREFA DE MONITORAMENTO DO ARQUIVO
        task_manager.register_task(task_name, self._tail_file, group="file_monitors", priority=TaskPriority.NORMAL, path=path, labels=labels, task_name=task_name)
        await task_manager.start_task(task_name)
        self.monitored_files[str_path] = {"task_name": task_name, "path": path}
        self.logger.debug("Arquivo registrado para monitoramento", task_name=task_name, path=str_path)
        return task_name
    async def remove_temporary_file(self, task_name: str) -> bool:
        self.logger.debug("Removendo arquivo monitorado", task_name=task_name)
        for p, details in list(self.monitored_files.items()):
            if details["task_name"] == task_name and task_name in task_manager.tasks:
                try:
                    task_manager.tasks[task_name].cancel()
                    await asyncio.gather(task_manager.tasks[task_name], return_exceptions=True)
                except Exception:
                    pass
                try:
                    metrics.FILE_INFO.remove(task_name, p)
                except Exception:
                    pass
                del self.monitored_files[p]
                return True
        return False
    def get_monitored_files(self) -> List[Dict]:
        self.logger.debug("Obtendo lista de arquivos monitorados")
        # Retorna lista simples com task_name e caminho do arquivo
        return [
            {"task_name": details["task_name"], "filepath": str(path)}
            for path, details in self.monitored_files.items()
        ]
    async def _tail_file(self, path: Path, labels: dict, task_name: str):
        self.logger.debug("Iniciando tail do arquivo", path=str(path), task_name=task_name)
        pos_file = self._get_position_file_path(path)
        last_save_time = time.time(); last_hb = time.time()
        metrics.FILE_INFO.labels(task_name=task_name, filepath=str(path)).set(1)
        try:
            async with aiofiles.open(path, 'r', encoding='utf-8', errors='replace') as f:
                self.logger.debug("Arquivo aberto para leitura", path=str(path))
                start_pos = await self._read_position(pos_file)
                if start_pos == 0:
                    await f.seek(0, 2); start_pos = await f.tell()
                else:
                    await f.seek(start_pos)
                current_position = start_pos
                # NOVO: contadores para amostragem de debug
                read_cnt = 0
                idle_cnt = 0
                while True:
                    line = await f.readline()
                    if line:
                        read_cnt += 1
                        if HOTPATH_DEBUG_ENABLED and HOTPATH_DEBUG_SAMPLE_N > 0 and (read_cnt % HOTPATH_DEBUG_SAMPLE_N == 0):
                            self.logger.debug("Linha lida do arquivo (amostrado)", path=str(path), task_name=task_name, read_count=read_cnt, line_preview=line[:80])
                        # ...existing code...
                    else:
                        idle_cnt += 1
                        if HOTPATH_DEBUG_ENABLED and HOTPATH_DEBUG_SAMPLE_N > 0 and (idle_cnt % HOTPATH_DEBUG_SAMPLE_N == 0):
                            self.logger.debug("Nenhuma nova linha (amostrado)", path=str(path), task_name=task_name, idle_count=idle_cnt)
                        await asyncio.sleep(0.5)
                        # ...existing code...
                    now = time.time()
                    if now - last_save_time > POSITION_SAVE_INTERVAL:
                        await self._save_position(pos_file, current_position)
                        last_save_time = now
        except asyncio.CancelledError:
            self.logger.debug("Tail do arquivo cancelado", path=str(path), task_name=task_name)
            pass
        except Exception as e:
            self.logger.error("Erro no tail do arquivo", path=str(path), task_name=task_name, error=str(e))
            raise
        finally:
            self.logger.debug("Finalizando tail do arquivo", path=str(path), task_name=task_name)
            if 'current_position' in locals():
                try:
                    await self._save_position(pos_file, current_position)
                except Exception:
                    pass
            try:
                metrics.FILE_INFO.remove(task_name, str(path))
            except Exception:
                pass

class DockerAPIRateLimiter:
    def __init__(self, requests_per_second: int, burst_size: int):
        self.rps = requests_per_second
        self.burst = burst_size
        self.tokens = float(burst_size)
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
    async def acquire(self):
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.burst, self.tokens + elapsed * self.rps)
            self.last_refill = now
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return
            wait_time = (1.0 - self.tokens) / float(self.rps or 1)
        await asyncio.sleep(max(0.0, wait_time))

class DockerConnectionPool:
    """
    Pool dinâmico: cria conexões sob demanda até max_size e encolhe quando houver ociosidade acima de min_size.
    Compatível com uso via contextmanager (get_connection()).
    """
    def __init__(self, min_size: int = 5, max_size: int = 25):
        self.min_size = min_size
        self.max_size = max_size
        self.available = asyncio.Queue(maxsize=max_size)
        self._all = set()
        self._initialized = False
        self._lock = asyncio.Lock()
    async def initialize(self):
        if self._initialized:
            return
        async with self._lock:
            if self._initialized:
                return
            for _ in range(self.min_size):
                conn = aiodocker.Docker()
                await conn.system.info()
                self._all.add(conn)
                await self.available.put(conn)
            self._initialized = True
    @asynccontextmanager
    async def get_connection(self):
        if not self._initialized:
            await self.initialize()
        conn = None
        async with self._lock:
            if not self.available.empty():
                conn = await self.available.get()
            elif len(self._all) < self.max_size:
                # cria nova conexão on-demand
                conn = aiodocker.Docker()
                await conn.system.info()
                self._all.add(conn)
            else:
                # espera uma ficar disponível
                conn = await self.available.get()
        try:
            yield conn
        finally:
            # devolve e tenta reduzir excesso ocioso
            try:
                await self.available.put(conn)
            except Exception:
                try:
                    await conn.close()
                except Exception:
                    pass
            await self._maybe_shrink()
    async def _maybe_shrink(self):
        async with self._lock:
            # reduz uma conexão se houver mais que min_size ociosas
            if self.available.qsize() > self.min_size and len(self._all) > self.min_size:
                try:
                    idle = await asyncio.wait_for(self.available.get(), timeout=0.01)
                except asyncio.TimeoutError:
                    return
                try:
                    await idle.close()
                except Exception:
                    pass
                try:
                    self._all.discard(idle)
                except Exception:
                    pass
    async def close_all(self):
        async with self._lock:
            while not self.available.empty():
                try:
                    await self.available.get()
                except Exception:
                    break
            for conn in list(self._all):
                try:
                    await conn.close()
                except Exception:
                    pass
            self._all.clear()
            self._initialized = False

class ContainerMonitor:
    def __init__(self, dispatcher):
        self.dispatcher = dispatcher
        self.docker = None
        self.monitored_containers: Dict[str, Dict[str, Any]] = {}
        self.lock = asyncio.Lock()
        self.logger = StructuredLogger("container_monitor")
        self.logger.debug("ContainerMonitor inicializado")
        # --- NOVO: backpressure/concurrency com semáforo ---
        self.max_concurrent = int(os.getenv("DOCKER_MAX_CONCURRENT_TAILS", "50"))
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        # Guardar timestamp do evento start para cálculo preciso do since
        self._event_since: Dict[str, int] = {}
        # --- NOVO: ignorar o próprio agente para evitar recursão/ruído ---
        self.ignore_self = os.getenv("DOCKER_IGNORE_SELF", "true").lower() == "true"
        self.self_id_prefix = os.getenv("HOSTNAME", "")[:12]
        # nomes comuns do próprio agente
        self.self_names = {os.getenv("SELF_CONTAINER_NAME", "log_capturer"), "log_capturer", "log-capturer"}
        # NOVO: tarefa de reconciliação
        self._reconciler_task: Optional[asyncio.Task] = None
        # NOVO: pool de conexões e rate limiter/caches
        self.docker_pool = DockerConnectionPool(min_size=DOCKER_CONNECTION_POOL_SIZE, max_size=DOCKER_CONNECTION_POOL_MAX_SIZE)
        self.docker_rate_limiter = DockerAPIRateLimiter(DOCKER_API_RATE_LIMIT, DOCKER_API_BURST_SIZE)
        self.container_metadata_cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ts: Dict[str, float] = {}

    def _labels_match_filter(self, labels: Dict[str, Any]) -> bool:
        """
        Suporta filtros simples via DOCKER_LABEL_FILTER:
        - "key=value" (igualdade)
        - "key" (presença)
        - múltiplos filtros separados por vírgula exigem todos verdadeiros (AND)
        """
        try:
            expr = (DOCKER_LABEL_FILTER or "").strip()
            if not expr:
                return True
            parts = [p.strip() for p in expr.split(",") if p.strip()]
            for p in parts:
                if "=" in p:
                    k, v = p.split("=", 1)
                    if str(labels.get(k, "")) != v:
                        return False
                else:
                    if p not in labels:
                        return False
            return True
        except Exception:
            return False

    async def start(self):
        self.logger.debug("Iniciando monitor de containers", docker_available=DOCKER_AVAILABLE)
        if not DOCKER_AVAILABLE:
            self.logger.warning("Biblioteca aiodocker não disponível; monitoramento de containers desativado")
            return
        docker_connected = await check_docker_connection()
        if not docker_connected:
            self.logger.warning("Conexão com Docker indisponível", connection_required=DOCKER_CONNECTION_REQUIRED)
            if DOCKER_CONNECTION_REQUIRED:
                self.logger.error("Docker connection required but failed, continuando sem container monitor")
            return
        try:
            self.docker = aiodocker.Docker()
            await self.docker_pool.initialize()
            task_manager.register_task("container_monitor", self._run_monitor, group="monitors", priority=TaskPriority.CRITICAL)
            await task_manager.start_task("container_monitor")
            # NOVO: iniciar reconciliador periódico
            if not self._reconciler_task:
                self._reconciler_task = asyncio.create_task(self._reconciler())
            self.logger.info("Tarefa principal de monitoramento registrada")
        except Exception as e:
            self.logger.error("Falha ao iniciar ContainerMonitor", error=str(e))

    async def stop(self):
        self.logger.debug("Parando monitor de containers")
        if self.docker:
            try:
                await self.docker.close()
            except Exception:
                pass
        if self._reconciler_task:
            self._reconciler_task.cancel()
            try:
                await self._reconciler_task
            except asyncio.CancelledError:
                pass
        await self.docker_pool.close_all()  # NOVO

    # NOVO: loop principal do monitor (varredura + eventos)
    async def _run_monitor(self):
        self.logger.debug("Loop principal do monitor de containers iniciado")
        while True:
            try:
                await task_manager.heartbeat("container_monitor")
                metrics.TASK_HEALTH.labels(task_name="container_monitor").set(1)
                self.logger.info("Varredura inicial de contêineres iniciada")
                started = await self._initial_container_scan()
                self.logger.info("Varredura inicial concluída", started_count=started)
                self.logger.info("Escutando eventos do Docker...")
                await self._listen_for_docker_events()
            except asyncio.CancelledError:
                self.logger.info("Monitor de containers cancelado")
                break
            except Exception as e:
                metrics.TASK_HEALTH.labels(task_name="container_monitor").set(0)
                self.logger.error("Falha no monitor; tentando novamente em 30s", error=str(e))
                await asyncio.sleep(30)

    # Helpers para padronizar aiodocker
    def _extract_container_id(self, container) -> Optional[str]:
        try:
            if isinstance(container, str):
                return container
            # aiodocker.DockerContainer normalmente possui _id
            if hasattr(container, "_id") and container._id:
                return container._id
            if hasattr(container, "id") and container.id:
                return container.id
            if isinstance(container, dict):
                return container.get("Id") or container.get("ID")
        except Exception:
            pass
        return None

    async def _get_container_labels(self, container) -> Dict[str, Any]:
        # Tenta obter labels do cache leve do objeto; caso não, usa .show()
        try:
            if hasattr(container, "_container") and container._container:
                labels = container._container.get("Labels") or {}
                if isinstance(labels, dict):
                    return labels
        except Exception:
            pass
        try:
            details = await container.show()
            labels = (details.get("Config", {}) or {}).get("Labels") or details.get("Labels") or {}
            return labels if isinstance(labels, dict) else {}
        except Exception:
            return {}

    # NOVO: varredura inicial
    async def _initial_container_scan(self) -> int:
        started_count = 0
        containers = await self.docker.containers.list()
        for c in containers:
            try:
                if await self._start_monitoring_container(c):
                    started_count += 1
            except Exception as e:
                self.logger.error("Erro ao iniciar monitoramento na varredura inicial", error=str(e))
        return started_count

    # NOVO: eventos do Docker tratados com verificação de estado
    async def _listen_for_docker_events(self):
        subscriber = self.docker.events.subscribe()
        try:
            while True:
                event = await subscriber.get()
                if event is None:
                    self.logger.warning("Stream de eventos Docker terminou")
                    break
                await task_manager.heartbeat("container_monitor")
                if event.get('Type') != 'container':
                    continue
                action = event.get('Action')
                actor_id = event.get('Actor', {}).get('ID')
                self.logger.debug("Evento Docker recebido", action=action, container=actor_id[:12] if actor_id else None)
                if not actor_id:
                    continue

                if action == 'start':
                    since_ts = None
                    if isinstance(event.get('time'), int):
                        since_ts = int(event['time'])
                    elif isinstance(event.get('timeNano'), int):
                        since_ts = int(event['timeNano'] // 1_000_000_000)
                    if since_ts:
                        self._event_since[actor_id] = since_ts
                        self.logger.debug("Timestamp do evento start registrado", container=actor_id[:12], since_event=since_ts)
                    try:
                        container_obj = await self.docker.containers.get(actor_id)
                        await self._start_monitoring_container(container_obj)
                    except Exception as e:
                        self.logger.error("Erro ao obter container em evento start", container=actor_id[:12], error=str(e))

                elif action in ('die', 'stop'):
                    try:
                        container_obj = await self.docker.containers.get(actor_id)
                        details = await container_obj.show()
                        is_running = details.get('State', {}).get('Running', False)
                        self.logger.debug("Verificando estado do container em evento de parada", container=actor_id[:12], running=is_running)
                        if not is_running:
                            await self._stop_monitoring_container(actor_id)
                        else:
                            self.logger.debug("Evento de parada recebido mas container ainda está up; ignorando cancelamento", container=actor_id[:12])
                    except Exception as e:
                        self.logger.error("Erro ao verificar estado no evento de parada", container=actor_id[:12], error=str(e))
                        await self._stop_monitoring_container(actor_id)
        finally:
            try:
                await subscriber.stop()
            except Exception:
                pass

    # NOVO: iniciar monitoramento de um container
    async def _start_monitoring_container(self, container) -> bool:
        # Padroniza como ID
        cid = self._extract_container_id(container)
        if not cid and container is not None:
            # Último recurso: tentar .show()
            try:
                details = await container.show()
                cid = details.get("Id")
            except Exception:
                cid = None
        if not cid:
            self.logger.warning("Não foi possível extrair ID do container; ignorando")
            return False

        async with self.lock:
            if cid in self.monitored_containers:
                self.logger.debug("Container já monitorado; ignorando", container=cid[:12])
                return False

            # Filtro por label (agora flexível)
            if DOCKER_LABEL_FILTER_ENABLED:
                if not DOCKER_LABEL_FILTER.strip():
                    self.logger.warning("Filtro de label habilitado mas vazio; não iniciando", container=cid[:12])
                    return False
                try:
                    labels = await self._get_container_labels(container)
                except Exception:
                    labels = {}
                if not self._labels_match_filter(labels):
                    self.logger.debug("Container filtrado por label; ignorando", container=cid[:12], filter=DOCKER_LABEL_FILTER, labels_preview=list(labels.keys())[:5])
                    return False

            task_name = f"container_{cid[:12]}"
            self.monitored_containers[cid] = {"task_name": task_name}
            task_manager.register_task(task_name, self._monitor_container, group="container_monitors", container_id=cid)
            await task_manager.start_task(task_name)
            available = getattr(self._semaphore, "_value", None)
            self.logger.info("Monitoramento de container iniciado", task_name=task_name, container=cid[:12], max_concurrent=self.max_concurrent, semaphore_available=available)
            return True

    # NOVO: parar monitoramento de um container
    async def _stop_monitoring_container(self, container_id: str):
        async with self.lock:
            if container_id in self.monitored_containers:
                task_name = self.monitored_containers[container_id].get("task_name")
                if task_name and task_name in task_manager.tasks:
                    task_manager.tasks[task_name].cancel()
                self.logger.info("Monitoramento de container parado", task_name=task_name, container=container_id[:12])

    # ADAPTADO: persistência de posição (since) por container usando ID completo (com fallback)
    async def _read_container_since(self, container_id: str) -> Optional[int]:
        try:
            path_full = POSITIONS_DIR / f"docker_{container_id}.since"
            path_short = POSITIONS_DIR / f"docker_{container_id[:12]}.since"
            if path_full.exists():
                async with aiofiles.open(path_full, "r") as f:
                    content = await f.read()
                    return int(content.strip() or "0")
            if path_short.exists():
                # Compatibilidade: migrar arquivo antigo para o nome com ID completo
                async with aiofiles.open(path_short, "r") as f:
                    content = await f.read()
                since_val = int((content or "0").strip() or "0")
                try:
                    # tentar renomear para o novo nome
                    os.replace(str(path_short), str(path_full))
                except Exception:
                    # fallback: gravar conteúdo no novo arquivo
                    async with aiofiles.open(path_full, "w") as fw:
                        await fw.write(str(since_val))
                return since_val
            return None
        except Exception:
            return None

    async def _save_container_since(self, container_id: str, since_ts: int):
        try:
            path_full = POSITIONS_DIR / f"docker_{container_id}.since"
            path_short = POSITIONS_DIR / f"docker_{container_id[:12]}.since"
            # Escrita atômica no arquivo full
            await _atomic_write(path_full, str(int(since_ts)))
            # limpar arquivo antigo com ID truncado, se existir
            try:
                if path_short.exists():
                    os.remove(path_short)
            except Exception:
                pass
        except Exception:
            pass

    # ADICIONADO: reconciliador periódico para alinhar tasks x containers
    async def _reconciler(self, interval: int = 60):
        while True:
            try:
                await asyncio.sleep(interval)
                if not self.docker:
                    continue
                containers = await self.docker.containers.list()
                running_ids: set[str] = set()
                for c in containers:
                    cid = self._extract_container_id(c)
                    if not cid:
                        try:
                            details = await c.show()
                            cid = details.get("Id")
                        except Exception:
                            cid = None
                    if not cid:
                        continue
                    running_ids.add(cid)
                    if cid not in self.monitored_containers:
                        try:
                            await self._start_monitoring_container(c)
                        except Exception as e:
                            self.logger.error("Erro ao iniciar monitoramento no reconciliador", container=cid[:12], error=str(e))
                # parar tasks de containers inexistentes
                async with self.lock:
                    for cid in list(self.monitored_containers.keys()):
                        if cid not in running_ids:
                            await self._stop_monitoring_container(cid)
            except asyncio.CancelledError:
                self.logger.info("Reconciliador cancelado")
                break
            except Exception as e:
                self.logger.error("Erro no reconciliador de containers", error=str(e))

    # CORRIGIDO: loop resiliente de tail sem trechos duplicados/fora de bloco
    async def _monitor_container(self, container_id: str):
        self.logger.debug("Monitorando container", container_id=container_id[:12])
        task_name = f"container_{container_id[:12]}"
        container_details = {}
        semaphore_acquired = False
        await self._semaphore.acquire()
        semaphore_acquired = True
        available = getattr(self._semaphore, "_value", None)
        self.logger.info("Semáforo adquirido para tail", task_name=task_name, semaphore_available=available)

        last_save_time = time.time()
        current_since: Optional[int] = None

        try:
            # 1) Obter detalhes iniciais via pool (chamada rápida)
            await self.docker_rate_limiter.acquire()
            async with self.docker_pool.get_connection() as docker_client:
                details = await docker_client.containers.container(container_id).show()
            name = details['Name'].lstrip('/')
            image = details['Config']['Image']

            if self.ignore_self and ((self.self_id_prefix and container_id.startswith(self.self_id_prefix)) or (name in self.self_names)):
                self.logger.info("Ignorando container do próprio agente", task_name=task_name, container=name, id_prefix=self.self_id_prefix)
                return

            container_details = {"task_name": task_name, "container_name": name, "image": image}
            metrics.CONTAINER_INFO.labels(**container_details).set(1)
            # Disponibiliza o container_name para o TaskManager usar nas métricas de restart
            try:
                task_manager.set_task_labels(task_name, {"container_name": name})
            except Exception:
                pass
            async with self.lock:
                self.monitored_containers.setdefault(container_id, {}).update(container_details)

            # 2) Calcular since (event/state/persistido)
            since_candidates: List[int] = []
            if container_id in self._event_since:
                evt_since = int(self._event_since.pop(container_id, 0) or 0)
                if evt_since > 0:
                    since_candidates.append(evt_since)
            try:
                started_at = details.get('State', {}).get('StartedAt')
                if started_at:
                    dt = datetime.fromisoformat(started_at.replace('Z', '+00:00'))
                    since_candidates.append(int(dt.timestamp()))
            except Exception:
                pass
            persisted = await self._read_container_since(container_id)
            if persisted and persisted > 0:
                since_candidates.append(int(persisted))
            if not since_candidates:
                since_candidates.append(max(0, int(time.time()) - 1))
            current_since = max(since_candidates)

            labels = {"container_id": container_id[:12], "container_name": name, "image": image}
            self.logger.info("Iniciando leitura de logs do container", task_name=task_name, container=name, image=image, since=current_since, since_candidates=since_candidates)

            # 3) Cliente dedicado para stream (não usa o pool)
            stream_client = None
            backoff = 1.0
            max_backoff = 30.0

            while True:
                # Verifica se está rodando usando pool (rápido)
                try:
                    await self.docker_rate_limiter.acquire()
                    async with self.docker_pool.get_connection() as docker_client:
                        det = await docker_client.containers.container(container_id).show()
                    is_running = det.get('State', {}).get('Running', False)
                except aiodocker.exceptions.DockerError as e:
                    if getattr(e, "status", None) == 404:
                        self.logger.warning("Container não encontrado, encerrando tarefa", task_name=task_name, container=container_id[:12])
                        break
                    self.logger.error("Erro ao consultar estado do container", task_name=task_name, error=str(e))
                    await task_manager.heartbeat(task_name)
                    await asyncio.sleep(min(backoff, max_backoff))
                    backoff = min(backoff * 2, max_backoff)
                    continue

                if not is_running:
                    self.logger.info("Container parou; finalizando monitoramento", task_name=task_name, container=container_id[:12])
                    break

                # Abre stream com cliente dedicado (não bloqueia o pool)
                try:
                    if stream_client is None:
                        stream_client = aiodocker.Docker()
                    container_stream = stream_client.containers.container(container_id)
                    stream = container_stream.log(stdout=True, stderr=True, follow=True, since=current_since or 0, timestamps=True)
                    self.logger.debug("Stream de logs aberto", task_name=task_name, since=current_since)
                    backoff = 1.0
                except Exception as e:
                    self.logger.error("Falha ao abrir stream de logs; tentando novamente", task_name=task_name, error=str(e))
                    await task_manager.heartbeat(task_name)
                    await asyncio.sleep(min(backoff, max_backoff))
                    backoff = min(backoff * 2, max_backoff)
                    continue

                try:
                    while True:
                        await task_manager.heartbeat(task_name)
                        try:
                            timeout = TASK_HEALTH_CHECK_INTERVAL / 2.0
                            line = await asyncio.wait_for(stream.__anext__(), timeout=timeout)
                            if not line:
                                continue
                            parsed_msg = line
                            try:
                                ts_str, msg = line.split(' ', 1)
                                dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                                current_since = max(current_since or 0, int(dt.timestamp()))
                                parsed_msg = msg
                            except Exception:
                                current_since = max(current_since or 0, int(time.time()))
                            await self.dispatcher.handle("docker", container_id[:12], parsed_msg.strip(), labels)

                            now_ts = time.time()
                            if now_ts - last_save_time > POSITION_SAVE_INTERVAL and current_since is not None:
                                await self._save_container_since(container_id, current_since)
                                last_save_time = now_ts
                        except asyncio.TimeoutError:
                            # NOVO: amostragem de debug de ociosidade
                            if HOTPATH_DEBUG_ENABLED and HOTPATH_DEBUG_SAMPLE_N > 0:
                                # usa contador local no closure
                                idle_dbg = locals().get("_idle_dbg_cnt", 0) + 1
                                locals().update({"_idle_dbg_cnt": idle_dbg})
                                if idle_dbg % HOTPATH_DEBUG_SAMPLE_N == 0:
                                    self.logger.debug("Timeout de leitura (amostrado)", task_name=task_name, idle_debug_count=idle_dbg)
                            continue
                except StopAsyncIteration:
                    self.logger.info("Stream de logs encerrado", task_name=task_name, container=name)
                    await task_manager.heartbeat(task_name)
                    await asyncio.sleep(0.5)
                    continue
                except asyncio.CancelledError:
                    self.logger.info("Leitura de logs cancelada", task_name=task_name, container=name)
                    raise
                except Exception as e:
                    self.logger.error("Exceção no tail do container; tentando reabrir stream", task_name=task_name, container=name, error=str(e), exc_info=True)
                    await task_manager.heartbeat(task_name)
                    await asyncio.sleep(min(backoff, max_backoff))
                    backoff = min(backoff * 2, max_backoff)
                    continue
                finally:
                    # Fecha cliente do stream entre tentativas
                    if stream_client:
                        try:
                            await stream_client.close()
                        except Exception:
                            pass
                        stream_client = None

        except aiodocker.exceptions.DockerError as e:
            if getattr(e, "status", None) == 404:
                self.logger.warning("Container não encontrado durante monitoramento", container=container_id[:12], status=404)
            else:
                self.logger.error("Erro Docker ao monitorar container", container=container_id[:12], error=str(e))
                raise
        except Exception as e:
            self.logger.error("Falha inesperada no monitoramento do container", container=container_id[:12], error=str(e), exc_info=True)
        finally:
            # Persistir posição (since) se disponível
            try:
                if current_since is not None:
                    await self._save_container_since(container_id, current_since)
            except Exception:
                pass

            # Remover métrica do container
            if container_details:
                try:
                    metrics.CONTAINER_INFO.remove(
                        container_details['task_name'],
                        container_details['container_name'],
                        container_details['image']
                    )
                except Exception:
                    pass

            # Remover do mapa de containers monitorados
            async with self.lock:
                if container_id in self.monitored_containers:
                    del self.monitored_containers[container_id]

            # Liberar semáforo e logar, evitando dupla liberação
            try:
                if semaphore_acquired:
                    self._semaphore.release()
                    semaphore_acquired = False
                    released_left = getattr(self._semaphore, "_value", None)
                    self.logger.info("Semáforo liberado após tail", task_name=task_name, semaphore_available=released_left)
            except ValueError as e:
                self.logger.warning("Tentativa de liberar semáforo já liberado", task_name=task_name, error=str(e))
            except Exception as e:
                self.logger.error("Erro ao liberar semáforo", task_name=task_name, error=str(e))
            self.logger.info("Monitoramento do container finalizado", task_name=task_name, container=container_id[:12])
