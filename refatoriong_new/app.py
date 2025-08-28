"""
app.py - Aplicação principal do agente de monitoramento.
- Inicializa sinks, dispatcher, monitores, API REST.
- Gerencia ciclo de vida, shutdown, health check.
- Facilita configuração rápida para testes (DEBUG_MODE, SINGLE_FILE_MODE).
"""

import time
import asyncio
import traceback
import os
from pathlib import Path
from aiohttp import web
from prometheus_client import start_http_server
import yaml
from typing import Dict
from .robustness import StructuredLogger, CorrelationContext, ConfigValidator, HealthMetrics
from .optimization import http_session_pool, unified_cleanup_manager, optimized_file_executor
from .task_manager import task_manager
from .metrics import metrics
from .config import (
    API_PORT, METRICS_PORT, PROCESSING_ENABLED, PROCESSING_WORKERS, PIPELINE_CONFIG_FILE,
    LOKI_SINK_ENABLED, ELASTICSEARCH_SINK_ENABLED, SPLUNK_SINK_ENABLED, LOCAL_SINK_ENABLED,
    LOKI_SINK_QUEUE_SIZE, LOKI_BATCH_SIZE, LOKI_BATCH_TIMEOUT, LOKI_URL, LOKI_BUFFER_DIR, LOKI_BUFFER_MAX_SIZE_MB,
    ELASTICSEARCH_SINK_QUEUE_SIZE, ELASTICSEARCH_HOSTS, ELASTICSEARCH_INDEX_PREFIX, ELASTICSEARCH_BUFFER_DIR,
    SPLUNK_SINK_QUEUE_SIZE, SPLUNK_HEC_URL, SPLUNK_HEC_TOKEN_SECRET, SPLUNK_BUFFER_DIR,
    LOCAL_SINK_QUEUE_SIZE,
    FILE_MONITOR_ENABLED,
    FILES_CONFIG_PATH,
)
from .sinks import LokiSink, LocalFileSink, ElasticsearchSink, SplunkSink  # adicionar imports dos outros sinks
from .dispatcher import LogDispatcher
from .processing import PipelineConfig, LogProcessor
from .monitors import FileMonitor, ContainerMonitor  # <-- adicionar ContainerMonitor
from .secrets import SecretManager

class LogCapturerApp:
    """
    LogCapturerApp - Orquestra inicialização, ciclo de vida e shutdown do agente.
    - Setup: valida config, inicializa sinks, dispatcher, monitores.
    - Start: inicia TaskManager, API REST, monitores.
    - Stop: shutdown ordenado de todos componentes.
    - Facilita modo debug/single-file para testes rápidos.
    """
    def __init__(self):
        self.logger = StructuredLogger("app")
        self.sinks = {}
        self.dispatcher = None
        self.file_monitor = None
        self.executor = None
        self.api_runner = None
        self.shutdown_event = asyncio.Event()
        self.secret_manager = SecretManager()
        self.processor = None
        self.task_manager = task_manager
        self.http_session_pool = http_session_pool
        self.file_executor = optimized_file_executor
        self.cleanup_manager = unified_cleanup_manager
        self.config_validator = ConfigValidator()
        self.health_metrics = HealthMetrics()
        self.startup_time = time.time()
        self.health_metrics.register_component("main")
        self.health_metrics.register_component("sinks")
        self.health_metrics.register_component("api")
        self.container_monitor = None  # <-- adicionar atributo
    async def setup(self):
        CorrelationContext.set_correlation_id(CorrelationContext.generate_correlation_id())
        is_valid = self.config_validator.validate_all()
        if not is_valid:
            if False:  # ajuste se quiser modo estrito aqui
                raise RuntimeError("Configuration validation failed in strict mode")
        await self.http_session_pool.initialize()
        # Validação de configuração obrigatória dos sinks
        valid = self._validate_sinks_config()
        # Instanciação protegida dos sinks
        if LOKI_SINK_ENABLED:
            if valid.get('loki', False):
                try:
                    self.sinks['loki'] = LokiSink(LOKI_SINK_QUEUE_SIZE, LOKI_BATCH_SIZE, LOKI_BATCH_TIMEOUT, LOKI_URL, LOKI_BUFFER_DIR, LOKI_BUFFER_MAX_SIZE_MB)
                except Exception as e:
                    self.logger.error("Falha ao instanciar sink Loki", error=str(e))
            else:
                self.logger.error("Configuração inválida: Loki habilitado porém LOKI_URL inválido; sink será ignorado", url=LOKI_URL)
        if ELASTICSEARCH_SINK_ENABLED:
            if valid.get('elasticsearch', False):
                try:
                    self.sinks['elasticsearch'] = ElasticsearchSink(ELASTICSEARCH_SINK_QUEUE_SIZE, ELASTICSEARCH_HOSTS, ELASTICSEARCH_INDEX_PREFIX, ELASTICSEARCH_BUFFER_DIR)
                except Exception as e:
                    self.logger.error("Falha ao instanciar sink Elasticsearch", error=str(e))
            else:
                self.logger.error("Configuração inválida: Elasticsearch habilitado porém ELASTICSEARCH_HOSTS inválido; sink será ignorado", hosts=",".join(ELASTICSEARCH_HOSTS or []))
        if SPLUNK_SINK_ENABLED:
            if valid.get('splunk', False):
                try:
                    self.sinks['splunk'] = SplunkSink(SPLUNK_SINK_QUEUE_SIZE, SPLUNK_HEC_URL, SPLUNK_HEC_TOKEN_SECRET, SPLUNK_BUFFER_DIR, self.secret_manager)
                except Exception as e:
                    self.logger.error("Falha ao instanciar sink Splunk", error=str(e))
            else:
                has_token = bool(self.secret_manager.get_secret(SPLUNK_HEC_TOKEN_SECRET) or "")
                self.logger.error("Configuração inválida: Splunk habilitado porém URL/token inválidos; sink será ignorado", url=SPLUNK_HEC_URL, has_token=has_token)
        if LOCAL_SINK_ENABLED:
            self.sinks['localfile'] = LocalFileSink(LOCAL_SINK_QUEUE_SIZE)
        if PROCESSING_ENABLED:
            pipeline_config = PipelineConfig.load(PIPELINE_CONFIG_FILE)
            self.processor = LogProcessor(pipeline_config)
            self.processor.start(PROCESSING_WORKERS)
        self.dispatcher = LogDispatcher(self.sinks, self.processor)
        self.file_monitor = FileMonitor(self.dispatcher)
        self.container_monitor = ContainerMonitor(self.dispatcher)  # <-- instanciar
        await self.cleanup_manager.start()
        await self.dispatcher.start()
        # --- Monitoramento de arquivos controlado por variável de ambiente ---
        if FILE_MONITOR_ENABLED:
            await self._load_file_config()
        else:
            self.logger.info("Monitoramento de arquivos desativado por configuração (FILE_MONITOR_ENABLED=false)")
    def _validate_sinks_config(self) -> Dict[str, bool]:
        """
        Valida configurações obrigatórias por sink. Retorna mapa de 'sink_name' -> válido.
        - Loki: URL http(s) não vazia.
        - Elasticsearch: hosts não vazios e com esquema http(s).
        - Splunk: URL http(s) válida e token resolvido via SecretManager.
        """
        results: Dict[str, bool] = {}
        try:
            if LOKI_SINK_ENABLED:
                results['loki'] = bool(LOKI_URL) and str(LOKI_URL).startswith(("http://", "https://"))
            if ELASTICSEARCH_SINK_ENABLED:
                hosts = ELASTICSEARCH_HOSTS or []
                results['elasticsearch'] = bool(hosts) and any(h.startswith(("http://", "https://")) for h in hosts)
            if SPLUNK_SINK_ENABLED:
                token_val = self.secret_manager.get_secret(SPLUNK_HEC_TOKEN_SECRET) or ""
                results['splunk'] = bool(SPLUNK_HEC_URL and SPLUNK_HEC_URL.startswith(("http://", "https://")) and token_val)
        except Exception as e:
            self.logger.error("Erro durante validação de sinks", error=str(e))
        return results

    async def stop(self):
        # Ordem: parar tarefas e sinks antes de fechar recursos compartilhados (HTTP/file/cleanup)
        if self.container_monitor:  # <-- parar monitor de containers primeiro
            try:
                await self.container_monitor.stop()
            except Exception:
                pass
        try:
            await self.task_manager.stop()
        except Exception:
            pass
        try:
            for sink in self.sinks.values():
                try:
                    await sink.stop()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            if self.dispatcher:
                await self.dispatcher.stop()
        except Exception:
            pass
        try:
            if self.processor:
                self.processor.stop()
        except Exception:
            pass
        try:
            await self.http_session_pool.close()
        except Exception:
            pass
        try:
            await self.file_executor.stop()
        except Exception:
            pass
        try:
            await self.cleanup_manager.stop()
        except Exception:
            pass
        try:
            if self.api_runner:
                await self.api_runner.cleanup()
        except Exception:
            pass
    # Handlers
    async def handle_status(self, request):
        status = {
            "status": "running",
            "uptime_seconds": int(time.time() - self.startup_time),
            "sinks": {name: {"queue_size": sink.queue.qsize(), "queue_capacity": sink.queue.maxsize} for name, sink in self.sinks.items()},
        }
        return web.json_response(status)
    async def handle_health(self, request):
        return web.json_response({"status": "healthy"})
    async def handle_get_monitored_files(self, request):
        files = self.file_monitor.get_monitored_files() if self.file_monitor else []
        return web.json_response(files)
    async def handle_add_monitored_file(self, request):
        data = await request.json()
        path = data.get("path"); labels = data.get("labels", {})
        if not path: return web.json_response({"error": "Path is required"}, status=400)
        task_name = await self.file_monitor.add_temporary_file(Path(path), labels)
        return web.json_response({"task_name": task_name}, status=201)
    async def handle_remove_monitored_file(self, request):
        task_name = request.match_info['task_name']
        ok = await self.file_monitor.remove_temporary_file(task_name)
        return web.json_response({"status": "removed"} if ok else {"error": "File/Task not found"}, status=200 if ok else 404)
    async def handle_task_status(self, request):
        return web.json_response(self.task_manager.get_task_status())
    async def handle_detailed_health(self, request):
        checker = RobustHealthCheck(self)
        health = await checker.comprehensive_health_check()
        status_code = 200 if health.get("status") in ("healthy", "degraded") else 503
        return web.json_response(health, status=status_code)
    async def handle_config_validation(self, request):
        return web.json_response({"errors": self.config_validator.errors, "warnings": self.config_validator.warnings})
    async def handle_validate_config(self, request):
        validator = ConfigValidator(); valid = validator.validate_all()
        return web.json_response({"valid": valid, "errors": validator.errors, "warnings": validator.warnings})
    async def handle_loki_storage_metrics(self, request):
        """Endpoint para expor métricas de armazenamento do Loki."""
        try:
            # Tentar múltiplos caminhos para as métricas
            possible_paths = [
                Path("/tmp/monitoring/loki_metrics.prom"),
                Path("/tmp/loki_metrics/loki_metrics.prom"),
                Path("/var/lib/docker/volumes/ssw-monitoring-stack-new-project_loki-monitoring/_data/loki_metrics.prom"),
                # Verificar volume nomeado
                Path("/tmp/loki-monitoring/loki_metrics.prom"),
                # Buscar no diretório de monitoramento
                *list(Path("/tmp/monitoring").glob("*loki*.prom")),
                # Busca em volumes do Docker
                *list(Path("/var/lib/docker/volumes").glob("*loki*/_data/*.prom")),
                # Volume com hífen ou underscore
                Path("/var/lib/docker/volumes/loki-monitoring/_data/loki_metrics.prom"),
                Path("/var/lib/docker/volumes/loki_monitoring/_data/loki_metrics.prom")
            ]
            
            content = None
            found_path = None
            
            # Primeiro tenta os caminhos específicos
            for metrics_file in possible_paths:
                try:
                    if metrics_file.exists():
                        content = metrics_file.read_text()
                        found_path = metrics_file
                        break
                except Exception:
                    continue
            
            # Se não encontrou, tenta busca mais ampla
            if content is None:
                try:
                    # Busca recursiva em /tmp
                    for metrics_file in Path("/tmp").rglob("*loki*.prom"):
                        try:
                            if metrics_file.exists():
                                content = metrics_file.read_text()
                                found_path = metrics_file
                                break
                        except Exception:
                            continue
                except Exception:
                    pass
                    
            if content:
                self.logger.info(f"Métricas do Loki encontradas em {found_path}")
                return web.Response(text=content, content_type="text/plain")
            else:
                # Métricas básicas se arquivo não existir
                self.logger.warning("Arquivo de métricas do Loki não encontrado")
                return web.Response(
                    text="# Loki storage metrics not available\nloki_metrics_available 0\n",
                    content_type="text/plain"
                )
        except Exception as e:
            self.logger.error(f"Erro ao ler métricas do Loki: {str(e)}")
            return web.Response(
                text=f"# Error reading Loki metrics: {str(e)}\nloki_metrics_error 1\n",
                content_type="text/plain"
            )

    async def _load_file_config(self):
        """
        Carrega arquivos do YAML de configuração e inicia monitoramento se habilitado.
        Se arquivo ausente ou inválido, loga e não monitora nenhum arquivo.
        """
        if not FILE_MONITOR_ENABLED:
            self.logger.info("Monitoramento de arquivos desativado por configuração (FILE_MONITOR_ENABLED=false)")
            return
        try:
            config_path = Path(os.getenv("FILES_CONFIG_PATH", "/etc/log_capturer/files.yaml"))
            if not config_path.exists():
                self.logger.warning("Arquivo de configuração de arquivos não encontrado", config_path=str(config_path))
                return
            with open(config_path, "r") as f:
                try:
                    data = yaml.safe_load(f)
                except Exception as e:
                    self.logger.error("Falha ao ler arquivo de configuração YAML", error=str(e), config_path=str(config_path))
                    return
            files = data.get("files", []) if isinstance(data, dict) else []
            if not files or not isinstance(files, list):
                self.logger.warning("Configuração de arquivos inválida ou vazia", config_path=str(config_path))
                return
            for entry in files:
                path = entry.get("path")
                labels = entry.get("labels", {})
                if path:
                    await self.file_monitor.add_temporary_file(Path(path), labels)
                    self.logger.info("Arquivo registrado para monitoramento", path=path, labels=labels)
        except Exception as e:
            self.logger.error("Erro ao carregar configuração de arquivos", error=str(e))
    async def start(self):
        # Inicia TaskManager e servidor de métricas
        await self.task_manager.start()
        start_http_server(METRICS_PORT)
        # Inicializa sinks com proteção; tratar sinks críticos
        CRITICAL_SINKS = {'loki', 'localfile'}
        failed_critical_sinks = []
        for name in list(self.sinks.keys()):
            try:
                await self.sinks[name].start()
                self.logger.info("Sink iniciado com sucesso", sink=name)
            except Exception as e:
                self.logger.error("Falha ao iniciar sink", sink=name, error=str(e))
                if name in CRITICAL_SINKS:
                    failed_critical_sinks.append(name)
                try:
                    del self.sinks[name]
                except Exception:
                    pass
        if failed_critical_sinks:
            raise RuntimeError(f"Sinks críticos falharam: {failed_critical_sinks}")
        if not self.sinks:
            raise RuntimeError("Nenhum sink foi iniciado com sucesso")
        # Inicia monitor de containers
        if self.container_monitor:
            await self.container_monitor.start()
        # Sobe API HTTP
        api_app = web.Application()
        api_app.router.add_get("/status", self.handle_status)
        api_app.router.add_get("/monitored/files", self.handle_get_monitored_files)
        api_app.router.add_post("/monitor/file", self.handle_add_monitored_file)
        api_app.router.add_delete("/monitor/file/{task_name}", self.handle_remove_monitored_file)
        api_app.router.add_get("/health", self.handle_health)
        api_app.router.add_get("/task/status", self.handle_task_status)
        api_app.router.add_get("/health/detailed", self.handle_detailed_health)
        api_app.router.add_get("/config/validation", self.handle_config_validation)
        api_app.router.add_post("/config/validate", self.handle_validate_config)
        # NOVO: Endpoint para métricas de storage do Loki
        api_app.router.add_get("/loki-storage-metrics", self.handle_loki_storage_metrics)
        
        # CORRIGIDO: Criar uma instância de RobustHealthCheck para acessar os métodos de administração de tarefas
        health_checker = RobustHealthCheck(self)
        # CORRIGIDO: Utilizar os métodos da instância de RobustHealthCheck
        api_app.router.add_get("/admin/orphaned-tasks", health_checker.handle_list_orphaned_tasks)
        api_app.router.add_post("/admin/cleanup-orphaned-tasks", health_checker.handle_cleanup_orphaned_tasks)

        self.api_runner = web.AppRunner(api_app)
        await self.api_runner.setup()
        site = web.TCPSite(self.api_runner, '0.0.0.0', API_PORT)
        await site.start()
        self.logger.info("API HTTP iniciada", port=API_PORT)

# --- Health Check Robusto ---
class RobustHealthCheck:
    def __init__(self, app: LogCapturerApp):
        self.app = app
        self.logger = StructuredLogger("health_check")
    async def comprehensive_health_check(self) -> Dict[str, any]:
        start = time.time()
        data = {"status": "healthy", "components": {}, "issues": []}
        try:
            data["components"]["docker"] = await self._check_docker()
            data["components"]["sinks"] = await self._check_sinks()
            data["components"]["tasks"] = await self._check_tasks()
            data["components"]["resources"] = await self._check_resources()
            data["components"]["loki"] = await self._check_loki()
            data["status"] = self._overall_status(data["components"])
        except Exception as e:
            data["status"] = "error"
            data["issues"].append(str(e))
            self.logger.error("Erro no health check", error=str(e))
        finally:
            data["check_duration_ms"] = round((time.time() - start) * 1000, 2)
        return data
    async def _check_docker(self) -> Dict[str, any]:
        if not self.app.container_monitor or not self.app.container_monitor.docker:
            return {"status": "disabled", "message": "container monitor not enabled"}
        try:
            info = await asyncio.wait_for(self.app.container_monitor.docker.system.info(), timeout=5)
            return {"status": "healthy", "containers_running": info.get("ContainersRunning", 0)}
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
    async def _check_sinks(self) -> Dict[str, Dict[str, any]]:
        sinks = {}
        for name, sink in self.app.sinks.items():
            try:
                q = sink.queue.qsize(); cap = sink.queue.maxsize
                util = (q / cap) if cap else 0
                cb = getattr(getattr(sink, "circuit_breaker", None), "state", None)
                cb_state = getattr(cb, "name", "unknown").lower() if cb else "unknown"
                status = "healthy"
                issues = []
                if util > 0.9: status, issues = "degraded", issues + [f"queue {util:.0%}"]
                if cb_state == "open": status, issues = "unhealthy", issues + ["circuit open"]
                sinks[name] = {"status": status, "queue_utilization": round(util, 3), "circuit_breaker": cb_state, "issues": issues}
            except Exception as e:
                sinks[name] = {"status": "error", "error": str(e)}
        return sinks
    async def _check_tasks(self) -> Dict[str, any]:
        st = self.app.task_manager.get_task_status()
        failed = [n for n, t in st.items() if t["status"] in ("failed", "circuit_open", "timeout")]
        status = "healthy" if not failed else ("degraded" if len(failed) < max(1, len(st)//2) else "unhealthy")
        return {"status": status, "failed": failed, "total": len(st)}
    async def _check_resources(self) -> Dict[str, any]:
        try:
            import psutil
            cpu = psutil.cpu_percent(interval=0.05)
            mem = psutil.virtual_memory().percent
            disk = max((psutil.disk_usage(part.mountpoint).percent for part in psutil.disk_partitions(all=False)), default=0)
            status = "healthy"; issues = []
            if cpu > 90: status, issues = "degraded", issues + [f"cpu {cpu:.1f}%"]
            if mem > 90: status, issues = "degraded", issues + [f"mem {mem:.1f}%"]
            if disk > 95: status, issues = "unhealthy", issues + [f"disk {disk:.1f}%"]
            return {"status": status, "cpu_percent": cpu, "mem_percent": mem, "disk_percent": disk, "issues": issues}
        except Exception as e:
            return {"status": "error", "error": str(e)}
    async def _check_loki(self) -> Dict[str, any]:
        if "loki" not in self.app.sinks:
            return {"status": "disabled", "message": "loki sink not enabled"}
        try:
            session = await self.app.http_session_pool.get_session()
            try:
                url = self.app.sinks["loki"].url.replace("/loki/api/v1/push", "/ready")
                async with session.get(url, timeout=5) as resp:
                    return {"status": "healthy" if resp.status == 200 else "degraded", "http_status": resp.status}
            finally:
                await self.app.http_session_pool.return_session(session)
        except Exception as e:
            return {"status": "unhealthy", "error": str(e)}
    def _overall_status(self, comps: Dict[str, any]) -> str:
        vals = []
        for k, v in comps.items():
            if isinstance(v, dict) and "status" in v:
                vals.append(v["status"])
            elif isinstance(v, dict):
                vals.extend([sv.get("status") for sv in v.values() if isinstance(sv, dict) and "status" in sv])
        if any(s in ("error", "unhealthy") for s in vals): return "unhealthy"
        if any(s == "degraded" for s in vals): return "degraded"
        return "healthy"

    async def handle_cleanup_orphaned_tasks(self, request):
        """Remove tasks órfãs manualmente com verificações de segurança"""
        try:
            removed_count = 0
            skipped_count = 0
            orphaned_tasks = []
            active_containers_found = []
            
            # Obter URL do Docker da configuração
            from .config import DOCKER_SOCKET_URL
            
            # Obter lista de containers ativos do Docker
            async with self.http_session_pool.get_session() as session:
                async with session.get(f"{DOCKER_SOCKET_URL}/containers/json?all=true") as resp:
                    if resp.status == 200:
                        all_containers = await resp.json()
                        # Incluir containers parados também (podem ser restarted)
                        active_ids = {c['Id'][:12] for c in all_containers}
                    else:
                        return web.json_response({"success": False, "error": "Erro ao conectar com Docker API"}, status=500)
            
            # Identificar tasks órfãs com verificação dupla
            for task_name in list(self.task_manager.health_status.keys()):
                if task_name.startswith("container_"):
                    container_id = task_name.replace("container_", "")
                    
                    # Verificação 1: Lista de containers
                    if container_id in active_ids:
                        active_containers_found.append({
                            "task_name": task_name,
                            "container_id": container_id,
                            "reason": "Container encontrado na lista do Docker"
                        })
                        skipped_count += 1
                        continue
                    
                    # Verificação 2: API individual do container
                    container_active = await self.task_manager._is_container_active(container_id)
                    
                    if container_active:
                        active_containers_found.append({
                            "task_name": task_name,
                            "container_id": container_id,
                            "reason": "Container confirmado ativo via API"
                        })
                        skipped_count += 1
                        continue
                    
                    # Container confirmadamente órfão
                    orphaned_tasks.append(task_name)
            
            # Remover apenas tasks confirmadamente órfãs
            for task_name in orphaned_tasks:
                await self.task_manager._remove_task_permanently(task_name)
                removed_count += 1
            
            self.logger.info("Limpeza manual concluída", removed_tasks=removed_count, skipped_tasks=skipped_count)
            
            return web.json_response({
                "success": True,
                "removed_tasks": removed_count,
                "skipped_tasks": skipped_count,
                "orphaned_tasks": orphaned_tasks,
                "active_containers_preserved": active_containers_found,
                "message": f"Removidas {removed_count} tasks órfãs, preservadas {skipped_count} tasks de containers ativos"
            })
            
        except Exception as e:
            self.logger.error("Erro na limpeza de tasks órfãs", error=str(e))
            return web.json_response({"success": False, "error": str(e)}, status=500)

    async def handle_list_orphaned_tasks(self, request):
        """Lista tasks órfãs com verificações de segurança"""
        try:
            # Obter URL do Docker da configuração
            from .config import DOCKER_SOCKET_URL
            
            # Obter containers ativos (incluindo parados)
            async with self.http_session_pool.get_session() as session:
                async with session.get(f"{DOCKER_SOCKET_URL}/containers/json?all=true") as resp:
                    if resp.status == 200:
                        all_containers = await resp.json()
                        active_ids = {c['Id'][:12] for c in all_containers}
                    else:
                        return web.json_response({"success": False, "error": "Erro ao conectar com Docker API"}, status=500)
            
            # Classificar tasks
            orphaned_tasks = []
            active_tasks = []
            
            for task_name in self.task_manager.health_status.keys():
                if task_name.startswith("container_"):
                    container_id = task_name.replace("container_", "")
                    task_status = self.task_manager.get_task_status().get(task_name, {})
                    
                    # Verificação dupla
                    in_docker_list = container_id in active_ids
                    container_active = await self.task_manager._is_container_active(container_id)
                    
                    task_info = {
                        "task_name": task_name,
                        "container_id": container_id,
                        "status": task_status,
                        "in_docker_list": in_docker_list,
                        "container_active": container_active
                    }
                    
                    if container_active or in_docker_list:
                        active_tasks.append(task_info)
                    else:
                        orphaned_tasks.append(task_info)
            
            return web.json_response({
                "success": True,
                "orphaned_count": len(orphaned_tasks),
                "active_count": len(active_tasks),
                "orphaned_tasks": orphaned_tasks,
                "active_tasks": active_tasks
            })
            
        except Exception as e:
            self.logger.error("Erro ao listar tasks órfãs", error=str(e))
            return web.json_response({"success": False, "error": str(e)}, status=500)
