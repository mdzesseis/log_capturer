import os
import json
import time
import aiofiles
import hashlib
import gzip
import shutil
import asyncio
import traceback
from collections import deque
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from .metrics import metrics
from .optimization import AdaptiveThrottler, optimized_file_executor, http_session_pool
from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitBreakerState
from .task_manager import task_manager, TaskPriority
from .robustness import ExponentialBackoffRetry, StructuredLogger, CorrelationContext
from .config import (
    BASE_DIR, BACKPRESSURE_THRESHOLD,
    LOKI_URL, LOCAL_SINK_BATCH_SIZE,
    SELF_ID_SHORT, SELF_CONTAINER_NAME, SELF_FEEDBACK_GUARD,
    PROACTIVE_CLEANUP_ENABLED, 
    LOCAL_SINK_MAX_SIZE_MB,
    # NOVO: limites duros para o batch do Loki
    LOKI_BATCH_HARD_MAX, LOKI_BATCH_HARD_MIN,
    # NOVO: rate limit/compressão
    SINK_RATE_LIMIT_ENABLED, SINK_RATE_LIMIT_BYTES_PER_TOKEN,
    SINK_HTTP_COMPRESSION_ENABLED, SINK_HTTP_COMPRESSION_MIN_BYTES, SINK_HTTP_COMPRESSION_LEVEL,
    # incluir alvo de latência no import para evitar NameError
    SINK_RATE_LIMIT_LATENCY_TARGET_MS,
    # NOVO: hot-path debug
    HOTPATH_DEBUG_ENABLED, HOTPATH_DEBUG_SAMPLE_N,
    # NOVO: seleção de algoritmo
    SINK_HTTP_COMPRESSION_ALGO, DISK_BUFFER_COMPRESSION_ALGO, DISK_BUFFER_COMPRESSION_LEVEL,
    # NOVO: compressão adaptativa
    SINK_HTTP_COMPRESSION_ADAPTIVE,
    # NOVO: DLQ por rejeição de timestamp no Loki
    TIMESTAMP_CLAMP_DLQ,
    # NOVO: rotação do arquivo local por tamanho
    LOCAL_SINK_MAX_FILE_SIZE_MB,
    # NOVO: variável para habilitar persistência e recovery
    SINK_PERSISTENCE_ENABLED,
    SINK_PERSISTENCE_RECOVERY_MAX_RETRIES,
    SINK_PERSISTENCE_RECOVERY_BACKOFF_BASE,
    SINK_PERSISTENCE_RECOVERY_BACKOFF_MAX,
    # NOVO: flag para permitir clamp automático de timestamps no sink (evitar NameError)
    TIMESTAMP_CLAMP_ENABLED,
)
from .optimization import AdaptiveRateLimiter  # NOVO

class DiskQuotaManager:
    def __init__(self, directory: Path, max_size_bytes: int):
        self.directory = directory
        self.max_size_bytes = max_size_bytes
        self.logger = StructuredLogger("disk_quota")
    
    async def _dir_size(self) -> int:
        total = 0
        try:
            for p in self.directory.rglob('*'):
                if p.is_file():
                    total += p.stat().st_size
        except Exception as e:
            self.logger.error("Erro ao calcular tamanho do diretório", error=str(e))
        return total
    
    async def enforce_quota(self) -> int:
        try:
            size = await self._dir_size()
            if size <= self.max_size_bytes:
                return 0
            target = int(self.max_size_bytes * 0.8)
            to_free = size - target
            freed = 0
            removed_cnt = 0
            files = []
            for f in self.directory.rglob('*.jsonl'):
                if f.is_file():
                    files.append((f.stat().st_mtime, f, f.stat().st_size))
            files.sort()
            for _, f, s in files:
                if freed >= to_free:
                    break
                try:
                    f.unlink()
                    freed += s
                    removed_cnt += 1
                except Exception as e:
                    self.logger.warning(f"Não foi possível remover arquivo {f}", error=str(e))
            if freed:
                metrics.CLEANUP_BYTES_FREED.inc(freed)
            if removed_cnt:
                metrics.CLEANUP_FILES_REMOVED.inc(removed_cnt)
            return freed
        except Exception:
            return 0

class AdaptiveBatcher:
    def __init__(self, min_size=50, max_size=1000, target_latency=2.0, hard_max: Optional[int] = None):
        self.min_size = max(1, int(min_size))
        self.max_size = max(self.min_size, int(max_size))
        self.hard_max = max(1, int(hard_max)) if hard_max is not None else self.max_size
        self.target_latency = max(0.1, float(target_latency))
        self.current_size = self.min_size
        self.recent_latencies = deque(maxlen=10)
        self.last_adjustment = time.time()
    
    def adjust_batch_size(self, latency_seconds: float):
        self.recent_latencies.append(max(0.0, float(latency_seconds)))
        if time.time() - self.last_adjustment < 30 or not self.recent_latencies:
            return
        avg = sum(self.recent_latencies) / len(self.recent_latencies)
        if avg > self.target_latency * 1.2:
            self.current_size = max(self.min_size, int(self.current_size * 0.8))
        elif avg < self.target_latency * 0.8:
            self.current_size = int(self.current_size * 1.2)
        # CLAMP explícito com limites máximos definidos
        self.current_size = max(self.min_size, min(self.current_size, self.max_size, self.hard_max))
        self.last_adjustment = time.time()
    
    def get_optimal_size(self) -> int:
        # Sempre retorna valor dentro dos limites
        return max(self.min_size, min(int(self.current_size), self.max_size, self.hard_max))

class AbstractSink:
    def __init__(self, name: str, queue_size: int, buffer_dir, max_buffer_size_mb: int = 0):
        import asyncio
        self.name = name
        self.queue = asyncio.Queue(maxsize=queue_size)
        self.buffer_dir = buffer_dir
        self.buffer_dir.mkdir(parents=True, exist_ok=True)
        self.max_buffer_size_mb = max_buffer_size_mb
        self.circuit_breaker = CircuitBreaker(f"{name}_sink", CircuitBreakerConfig())
        self.throttler = AdaptiveThrottler(name)
        self.recent_errors = 0
        self.error_window = []
        self.retry_client = ExponentialBackoffRetry(max_retries=3)
        self.logger = StructuredLogger(f"sink_{name}")
        
        # NOVO: cota por sink e limpeza proativa opcional
        self.disk_quota_manager = DiskQuotaManager(buffer_dir, max_buffer_size_mb * 1024 * 1024) if max_buffer_size_mb > 0 else None
        self._cleanup_task = None
        
        # NOVO: contador de debug para amostragem em hot-path
        self._hp_dbg_counter = 0
        
        # NOVO: Sistema de persistência opcional controlado por variável de ambiente
        self._persistence_enabled = SINK_PERSISTENCE_ENABLED
        self._shutdown_event = asyncio.Event()
        self.persistence_path = None
        if self._persistence_enabled:
            self.persistence_path = os.path.join(buffer_dir, f"{name}_persistence.json")
            self.logger.info("Sistema de persistência habilitado", persistence_path=self.persistence_path)
        
        # NOVO: Métricas estendidas para observabilidade
        self._stats = {
            'batches_processed': 0,
            'batches_failed': 0,
            'logs_written': 0,
            'logs_sent': 0,
            'recovery_operations': 0,
            'persistence_operations': 0
        }
        
        self.logger.info("AbstractSink inicializado", 
                        name=name, 
                        persistence_enabled=self._persistence_enabled,
                        buffer_dir=str(buffer_dir),
                        max_buffer_size_mb=max_buffer_size_mb)

    def _is_self_batch(self, batch: List[Dict]) -> bool:
        if not SELF_FEEDBACK_GUARD:
            return False
        try:
            for item in batch:
                meta = item.get('meta', {})
                labels = item.get('labels', {})
                if meta.get('source_type') == 'docker':
                    if meta.get('source_id') == SELF_ID_SHORT:
                        return True
                    if labels.get('container_name') == SELF_CONTAINER_NAME:
                        return True
        except Exception:
            return False
        return False

    async def start(self):
        from .optimization import unified_cleanup_manager
        self.logger.debug("Iniciando sink", name=self.name, buffer_dir=str(self.buffer_dir), max_buffer_size_mb=self.max_buffer_size_mb)
        
        if self.max_buffer_size_mb > 0:
            unified_cleanup_manager.register_cleanup_target(self.buffer_dir, self.max_buffer_size_mb, ['*.jsonl','*.processing','*.tmp'])
        
        if self.disk_quota_manager and PROACTIVE_CLEANUP_ENABLED:
            import asyncio
            self._cleanup_task = asyncio.create_task(self._proactive_cleanup_loop())
        
        await self._register_tasks()
        self.logger.debug("Sink registrado no TaskManager", name=self.name)

    async def stop(self): 
        self.logger.debug("Parando sink", name=self.name)
        
        # NOVO: Sinalizar shutdown para worker loops
        self._shutdown_event.set()
        
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        
        # NOVO: Log de estatísticas finais
        self.logger.info("Estatísticas finais do sink", 
                        name=self.name, 
                        stats=self._stats)

    async def _proactive_cleanup_loop(self):
        while True:
            try:
                # Opcional: aplicar limpeza proativa via quota
                if self.disk_quota_manager:
                    await self.disk_quota_manager.enforce_quota()
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(5)

    async def compress_buffer_files(self, older_than_seconds: Optional[int] = None, min_size_bytes: int = 64 * 1024, max_files: int = 10):
        """
        Comprimir arquivos antigos do buffer (.jsonl -> .jsonl.{gz|zst|lz4}) para grandes volumes.
        - older_than_seconds: idade mínima do arquivo para compressão (env: BUFFER_COMPRESS_AFTER_SECONDS, default 600s)
        - min_size_bytes: evita comprimir arquivos muito pequenos (default 64KiB)
        - max_files: limite de arquivos por execução (env: BUFFER_COMPRESS_MAX_FILES, default 10)
        """
        try:
            older_than_seconds = older_than_seconds or int(os.getenv("BUFFER_COMPRESS_AFTER_SECONDS", "600"))
            max_files = int(os.getenv("BUFFER_COMPRESS_MAX_FILES", str(max_files)))
        except Exception:
            older_than_seconds = older_than_seconds or 600
        cutoff = time.time() - older_than_seconds

        # NOVO: threshold dinâmico para pular arquivos pequenos quando disco está saudável
        try:
            usage = shutil.disk_usage(self.buffer_dir)
            usage_pct = usage.used / max(1, usage.total)
            if usage_pct < 0.70:
                dyn_min = 256 * 1024  # 256KiB
                if dyn_min > min_size_bytes:
                    self.logger.info("Compressão: elevando threshold mínimo por disco saudável", previous=min_size_bytes, new=dyn_min, usage_percent=round(usage_pct*100,1))
                    min_size_bytes = dyn_min
            else:
                self.logger.debug("Compressão: mantendo threshold mínimo", min_size_bytes=min_size_bytes, usage_percent=round(usage_pct*100,1))
        except Exception:
            pass

        candidates = []
        try:
            for f in self.buffer_dir.glob("*.jsonl"):
                try:
                    if not f.is_file():
                        continue
                    # pular arquivos em processamento e já comprimidos
                    if str(f).endswith(".processing"):
                        continue
                    # já existe alguma versão comprimida?
                    if f.with_suffix(f.suffix + ".gz").exists() or f.with_suffix(f.suffix + ".zst").exists() or f.with_suffix(f.suffix + ".lz4").exists():
                        continue
                    st = f.stat()
                    if st.st_mtime > cutoff:
                        continue
                    if st.st_size < min_size_bytes:
                        continue
                    candidates.append((st.st_mtime, f, st.st_size))
                except Exception:
                    continue
        except Exception:
            return
        candidates.sort()

        # NOVO: compressão por algoritmo (streaming, baixa memória)
        def _compress_sync(src: Path, dst: Path, algo: str, level: int):
            algo = (algo or "zstd").lower()
            if algo in ("zstd", "zst"):
                try:
                    import zstd
                    cctx = zstd.ZstdCompressor(level=int(level))
                    with open(src, "rb") as fin, open(dst, "wb") as fout, cctx.stream_writer(fout) as compressor:
                        shutil.copyfileobj(fin, compressor, length=1024 * 1024)
                    return
                except Exception as e:
                    # fallback para gzip
                    pass
            if algo == "lz4":
                try:
                    import lz4.frame as lz4f
                    with open(src, "rb") as fin, lz4f.open(dst, "wb", compression_level=int(level)) as fout:
                        shutil.copyfileobj(fin, fout, length=1024 * 1024)
                    return
                except Exception:
                    pass
            # gzip como padrão/fallback
            with open(src, "rb") as fin, gzip.open(dst, "wb", compresslevel=int(level if level else 6)) as fout:
                shutil.copyfileobj(fin, fout, length=1024 * 1024)

        processed = 0
        saved_total = 0
        # extensão conforme algoritmo
        ext = ".zst" if DISK_BUFFER_COMPRESSION_ALGO in ("zstd","zst") else (".lz4" if DISK_BUFFER_COMPRESSION_ALGO == "lz4" else ".gz")
        for _, f, orig_size in candidates:
            if processed >= max_files:
                break
            comp_path = f.with_suffix(f.suffix + ext)
            try:
                await asyncio.to_thread(_compress_sync, f, comp_path, DISK_BUFFER_COMPRESSION_ALGO, DISK_BUFFER_COMPRESSION_LEVEL)
                new_size = comp_path.stat().st_size if comp_path.exists() else orig_size
                if new_size < orig_size:
                    try:
                        f.unlink()
                    except Exception:
                        pass
                    saved_total += (orig_size - new_size)
                else:
                    try:
                        comp_path.unlink()
                    except Exception:
                        pass
                processed += 1
            except Exception:
                try:
                    if comp_path.exists():
                        comp_path.unlink()
                except Exception:
                    pass
                continue
        if processed or saved_total:
            try:
                if saved_total:
                    metrics.CLEANUP_BYTES_FREED.inc(saved_total)
            except Exception:
                pass
            self.logger.info("Compressão de buffer concluída", sink=self.name, files=processed, bytes_saved=saved_total)

    async def _register_tasks(self):
        self.logger.debug("Registrando tarefas do sink", name=self.name)
        if hasattr(self, '_worker_loop'):
            task_manager.register_task(f"{self.name}_worker", self._worker_loop, group=f"sink_{self.name}", priority=TaskPriority.HIGH)
            await task_manager.start_task(f"{self.name}_worker")
        if hasattr(self, '_update_buffer_metric'):
            task_manager.register_task(f"{self.name}_metrics", self._update_buffer_metric, group=f"sink_{self.name}", priority=TaskPriority.LOW)
            await task_manager.start_task(f"{self.name}_metrics")
        # Tarefa dedicada de recuperação de batches persistidos (não bloqueia o worker principal)
        if self._persistence_enabled and hasattr(self, '_recovery_loop'):
            task_manager.register_task(f"{self.name}_persistence_recovery", self._recovery_loop, group=f"sink_{self.name}", priority=TaskPriority.LOW)
            await task_manager.start_task(f"{self.name}_persistence_recovery")

    async def _send_heartbeat(self):
        self.logger.debug("Enviando heartbeat do sink", name=self.name)
        await task_manager.heartbeat(f"{self.name}_worker")
        await task_manager.heartbeat(f"{self.name}_metrics")

    async def send(self, log_entry: Dict):
        # NOVO: amostragem de debug
        self._hp_dbg_counter += 1
        if HOTPATH_DEBUG_ENABLED and HOTPATH_DEBUG_SAMPLE_N > 0 and (self._hp_dbg_counter % HOTPATH_DEBUG_SAMPLE_N == 0):
            self.logger.debug("Enviando log para fila do sink (amostrado)", name=self.name, queue_size=self.queue.qsize(), dbg_count=self._hp_dbg_counter)
        try:
            queue_load = self.queue.qsize() / self.queue.maxsize
            await self.throttler.throttle(queue_load, self.recent_errors)
            if queue_load > BACKPRESSURE_THRESHOLD:
                metrics.THROTTLING_DELAY.labels(sink_name=self.name).observe(self.throttler.current_delay)
            self.queue.put_nowait(log_entry)
            if HOTPATH_DEBUG_ENABLED and HOTPATH_DEBUG_SAMPLE_N > 0 and (self._hp_dbg_counter % HOTPATH_DEBUG_SAMPLE_N == 0):
                self.logger.debug("Log enfileirado (amostrado)", name=self.name, queue_size=self.queue.qsize(), dbg_count=self._hp_dbg_counter)
            metrics.SINK_QUEUE_SIZE.labels(sink_name=self.name).set(self.queue.qsize())
        except Exception:
            self.logger.debug("Fila cheia, overflow para disco", name=self.name)
            await self._optimized_overflow_to_disk([log_entry])
            metrics.SINK_QUEUE_SIZE.labels(sink_name=self.name).set(self.queue.qsize())

    async def _optimized_overflow_to_disk(self, batch: List[Dict]):
        self.logger.debug("Overflow otimizado para disco", name=self.name, batch_size=len(batch))
        # 1) Enforce de cota configurada
        if self.disk_quota_manager:
            try:
                freed = await self.disk_quota_manager.enforce_quota()
                if freed > 0:
                    self.logger.warning("Cota de disco atingida, limpeza aplicada", bytes_freed=freed, sink=self.name)
            except Exception:
                pass
        # 2) Verifica espaço livre no filesystem com margem
        try:
            statvfs = os.statvfs(self.buffer_dir)
            free_bytes = statvfs.f_frsize * statvfs.f_bavail
        except Exception as e:
            # Se não conseguir medir, tenta mesmo assim
            free_bytes = None
            self.logger.warning("Falha ao obter espaço livre; seguindo com fallback", error=str(e))
        # Estima tamanho do batch (amostragem)
        try:
            sample = batch[:10]
            sample_size = sum(len(json.dumps(log)) for log in sample) or 1
            batch_size_estimate = int(sample_size * (len(batch) / max(1, len(sample))))
        except Exception:
            batch_size_estimate = 0
        if free_bytes is not None and batch_size_estimate > 0 and free_bytes < batch_size_estimate * 2:
            # Espaço insuficiente: descartar com métrica e log
            self.logger.error("Espaço em disco insuficiente para buffer", free_bytes=free_bytes, estimated_needed=batch_size_estimate, sink=self.name)
            try:
                metrics.PROCESSING_DROPPED.inc(len(batch))
            except Exception:
                pass
            return
        # 3) Grava em .jsonl (otimizado)
        fname = f"{datetime.utcnow().isoformat()}-{hashlib.md5(os.urandom(8)).hexdigest()}.jsonl"
        file_path = str(self.buffer_dir / fname)
        data_lines = [json.dumps(log) + "\n" for log in batch]
        ok = await optimized_file_executor.write_batch_optimized(file_path, data_lines)
        if not ok:
            await self._fallback_overflow_to_disk(batch)

    async def _fallback_overflow_to_disk(self, batch: List[Dict]):
        self.logger.debug("Overflow fallback para disco", name=self.name, batch_size=len(batch))
        fname = str(self.buffer_dir / f"{datetime.utcnow().isoformat()}-{hashlib.md5(os.urandom(8)).hexdigest()}.jsonl.tmp")
        async with aiofiles.open(fname, 'w', encoding='utf-8') as f:
            await f.write("\n".join(json.dumps(log) for log in batch))
        os.rename(fname, fname.replace('.tmp',''))

    def _track_error(self):
        self.logger.debug("Rastreando erro no sink", name=self.name, recent_errors=self.recent_errors)
        now = time.time()
        self.error_window.append(now)
        cutoff = now - 300
        self.error_window = [t for t in self.error_window if t > cutoff]
        self.recent_errors = len(self.error_window)

    async def _dir_size(self) -> int:
        total = 0
        try:
            for p in self.buffer_dir.rglob('*'):
                if p.is_file():
                    total += p.stat().st_size
        except Exception as e:
            self.logger.warning("Erro ao calcular tamanho do diretório", error=str(e))
        return total

    async def _read_from_disk_buffer(self) -> Optional[Dict[str, Any]]:
        self.logger.debug("Lendo batch do buffer em disco", name=self.name)
        try:
            files = []
            files.extend([f for f in self.buffer_dir.glob('*.jsonl') if f.is_file()])
            files.extend([f for f in self.buffer_dir.glob('*.jsonl.gz') if f.is_file()])
            files.extend([f for f in self.buffer_dir.glob('*.jsonl.zst') if f.is_file()])
            files.extend([f for f in self.buffer_dir.glob('*.jsonl.lz4') if f.is_file()])
            files = sorted(files, key=os.path.getmtime)
            if not files:
                return None

            target_path = files[0]
            target_file = str(target_path)
            processing_file = target_file + '.processing'
            os.rename(target_file, processing_file)

            batch: List[Dict[str, Any]] = []

            if target_file.endswith('.jsonl.gz'):
                def _read_gz_sync(p: str) -> str:
                    import gzip
                    with gzip.open(p, 'rt', encoding='utf-8', errors='replace') as fin:
                        return fin.read()
                content = await asyncio.to_thread(_read_gz_sync, processing_file)
            elif target_file.endswith('.jsonl.zst'):
                def _read_zst_sync(p: str) -> str:
                    import zstandard as zstd
                    with open(p, 'rb') as fin:
                        dctx = zstd.ZstdDecompressor()
                        with dctx.stream_reader(fin) as r:
                            return r.read().decode('utf-8', errors='replace')
                content = await asyncio.to_thread(_read_zst_sync, processing_file)
            elif target_file.endswith('.jsonl.lz4'):
                def _read_lz4_sync(p: str) -> str:
                    import lz4.frame as lz4f
                    with lz4f.open(p, 'rb') as fin:
                        return fin.read().decode('utf-8', errors='replace')
                content = await asyncio.to_thread(_read_lz4_sync, processing_file)
            else:
                async with aiofiles.open(processing_file, 'r', encoding='utf-8') as f:
                    content = await f.read()

            batch = [json.loads(line) for line in content.splitlines() if line]
            return {"batch": batch, "file_path": processing_file, "restore_path": target_file}
        except Exception:
            return None

    async def _update_buffer_metric(self):
        while not self._shutdown_event.is_set():
            try:
                dir_size = await self._dir_size()
                metrics.SINK_DISK_BUFFER_BYTES.labels(sink_name=self.name).set(dir_size)
                
            except Exception as e:
                self.logger.warning("Erro ao atualizar métrica de buffer", error=str(e))
                
            await asyncio.sleep(30)

    async def _remove_persistence(self):
        """
        Remove arquivo de persistência após sucesso no processamento.
        """
        if not self._persistence_enabled or not self.persistence_path:
            return
            
        try:
            if os.path.exists(self.persistence_path):
                os.unlink(self.persistence_path)
        except Exception as e:
            self.logger.warning("Erro ao remover arquivo de persistência", 
                              persistence_path=self.persistence_path,
                              error=str(e))

    # NOVO: Sistema de persistência para recovery após falhas
    async def _load_persisted_batch(self) -> Optional[List[Dict]]:
        """
        Carrega batch persistido do disco para recovery.
        Retorna None se não há batch persistido ou se persistência está desabilitada.
        """
        if not self._persistence_enabled or not self.persistence_path:
            return None
            
        try:
            if not os.path.exists(self.persistence_path):
                return None
                
            async with aiofiles.open(self.persistence_path, 'r', encoding='utf-8') as f:
                content = await f.read()
                if not content.strip():
                    return None
                    
            batch_data = json.loads(content)
            if not batch_data or not isinstance(batch_data, list):
                return None
                
            self._stats['recovery_operations'] += 1
            self.logger.info("Batch recuperado da persistência", 
                           batch_size=len(batch_data),
                           persistence_path=self.persistence_path,
                           recovery_count=self._stats['recovery_operations'])
            
            return batch_data
            
        except Exception as e:
            self.logger.error("Erro ao carregar batch persistido", 
                            persistence_path=self.persistence_path, 
                            error=str(e))
            # Remove arquivo corrompido
            try:
                if os.path.exists(self.persistence_path):
                    os.remove(self.persistence_path)
                    self.logger.warning("Arquivo de persistência corrompido removido")
            except Exception:
                pass
            return None

    async def _persist_batch(self, batch: List[Dict]):
        """
        Persiste batch no disco para recovery posterior.
        Só opera se persistência estiver habilitada.
        """
        if not self._persistence_enabled or not self.persistence_path or not batch:
            return
            
        try:
            # Criar diretório se não existir
            os.makedirs(os.path.dirname(self.persistence_path), exist_ok=True)
            
            # Escrever de forma atômica
            temp_path = self.persistence_path + ".tmp"
            async with aiofiles.open(temp_path, 'w', encoding='utf-8') as f:
                await f.write(json.dumps(batch))
            
            # Renomear para garantir atomicidade
            os.rename(temp_path, self.persistence_path)
            
            self._stats['persistence_operations'] += 1
            self.logger.debug("Batch persistido para recovery", 
                            batch_size=len(batch),
                            persistence_path=self.persistence_path,
                            persistence_count=self._stats['persistence_operations'])
            
        except Exception as e:
            self.logger.error("Erro ao persistir batch", 
                            persistence_path=self.persistence_path, 
                            batch_size=len(batch),
                            error=str(e))
            # Limpar arquivo temporário em caso de erro
            try:
                temp_path = self.persistence_path + ".tmp"
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
            except Exception:
                pass

    async def send_batch(self, batch: List[Dict]) -> bool:
        """Envia batch para o destino - deve ser implementado pelas subclasses"""
        raise NotImplementedError("send_batch deve ser implementado pelas subclasses")

    async def _send_to_dlq(self, batch: List[Dict], reason: str):
        """Grava registros rejeitados em DLQ em disco com o motivo."""
        try:
            dlq_path = str(BASE_DIR / "dlq" / f"{self.name}_rejected.log")
            lines = []
            for item in batch:
                meta = dict(item.get("meta", {}))
                meta["is_dlq"] = True
                meta["dlq_reason"] = reason
                meta["dlq_sink"] = self.name
                entry = {
                    "ts": item.get("ts"),
                    "labels": item.get("labels"),
                    "line": item.get("line"),
                    "meta": meta
                }
                lines.append(json.dumps(entry) + "\n")
                try:
                    metrics.LOGS_DLQ.labels(source_type=meta.get("source_type", "unknown"), reason=reason).inc()
                except Exception:
                    pass
            await optimized_file_executor.write_batch_optimized(dlq_path, lines)
        except Exception:
            # Em último caso, ignora erro de DLQ para não bloquear o worker
            pass

class LokiSink(AbstractSink):
    def __init__(self, queue_size: int, batch_size: int, batch_timeout: float, url: str, buffer_dir, max_buffer_size_mb: int = 0):
        super().__init__("loki", queue_size, buffer_dir, max_buffer_size_mb)
        self.logger.debug("LokiSink inicializado", batch_size=batch_size, batch_timeout=batch_timeout, url=url)
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.url = url
        
        # NOVO: rate limiter adaptativo
        self.rate_limiter = AdaptiveRateLimiter()
        self.rate_limiter.set_logger(self.logger)

        # NOVO: limites duros e respeitando a capacidade da fila do sink
        queue_cap = int(self.queue.maxsize or 0)
        # candidatos: config, env, capacidade da fila (90%), teto seguro absoluto (5000)
        hard_max_candidates = [int(self.batch_size), int(LOKI_BATCH_HARD_MAX)]
        if queue_cap > 0:
            hard_max_candidates.append(int(queue_cap * 0.9))
        hard_max = max(1, min(min(hard_max_candidates), 5000))
        hard_min = max(1, min(int(LOKI_BATCH_HARD_MIN), hard_max))
        min_size = min(max(50, int(self.batch_size * 0.25)), hard_max)

        # batch adaptativo com limites explícitos
        self._batcher = AdaptiveBatcher(
            min_size=max(min_size, hard_min),
            max_size=hard_max,
            target_latency=self.batch_timeout,
            hard_max=hard_max,
        )
        self._recovery_task = None
    async def start(self):
        await super().start()
        self.logger.debug("LokiSink start concluído")

    async def _recovery_loop(self):
        """Tarefa separada que processa batches persistidos sem bloquear o worker principal."""
        self.logger.info("Recovery loop do LokiSink iniciado", persistence_path=self.persistence_path)
        backoff_base = float(SINK_PERSISTENCE_RECOVERY_BACKOFF_BASE or 1.0)
        backoff_max = float(SINK_PERSISTENCE_RECOVERY_BACKOFF_MAX or 30.0)
        max_retries = int(SINK_PERSISTENCE_RECOVERY_MAX_RETRIES or 10)
        while not self._shutdown_event.is_set():
            try:
                batch = await self._load_persisted_batch()
                if not batch:
                    await asyncio.sleep(1.0)
                     # Adiciona heartbeat mesmo em ciclos sem batch         
                    await self._send_heartbeat()
                    continue

                self.logger.info("Iniciando tentativa de recovery do batch persistido", batch_size=len(batch))
                attempt = 0
                current_backoff = backoff_base
                success = False
                while attempt < max_retries and not self._shutdown_event.is_set():
                    attempt += 1
                    try:
                        # Adiciona heartbeat em ciclos com batch
                        await self._send_heartbeat()
                        ok = await self.send_batch(batch)
                        if ok:
                            self.logger.info("Recovery do batch persistido bem-sucedido", attempts=attempt, batch_size=len(batch))
                            success = True
                            await self._remove_persistence()
                            break
                        else:
                            await self._send_heartbeat()
                            self.logger.warning("Recovery do batch persistido falhou (retry)", attempt=attempt, batch_size=len(batch))
                    except Exception as e:
                        self.logger.error("Exceção ao tentar recovery do batch persistido", attempt=attempt, error=str(e))
                    await asyncio.sleep(min(current_backoff, backoff_max))
                    current_backoff = min(current_backoff * 2.0, backoff_max)

                if not success:
                    # Após retries, mover para DLQ se configurado, senão remover persistência
                    if TIMESTAMP_CLAMP_DLQ:
                        try:
                            await self._send_heartbeat()
                            await self._send_to_dlq(batch, f"persistence_exhausted_{attempt}")
                        except Exception as e:
                            self.logger.error("Falha ao mover batch persistido para DLQ", error=str(e))
                    # Sempre remover persistência após todos os retries, independente do DLQ
                    try:
                        await self._send_heartbeat()
                        await self._remove_persistence()
                    except Exception:
                        pass
                await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                await self._send_heartbeat()
                self.logger.error("Erro no recovery loop do LokiSink", error=str(e))
                await asyncio.sleep(1.0)
        self.logger.info("Recovery loop do LokiSink finalizado")

    async def send_batch(self, batch: List[Dict]) -> bool:
        """Envia batch para o Loki"""
        self.logger.debug("Enviando batch para Loki", batch_size=len(batch))
        
        if self._is_self_batch(batch):
            self.logger.debug("Ignorando batch do próprio agente")
            return True
            
        try:
            # Preparação do payload
            streams = {}
            for record in batch:
                ts = record.get('ts', int(time.time() * 1e9))
                line = record.get('line', '')
                labels = record.get('labels', {})
                
                # Converte labels para o formato do Loki "{key="value",key2="value2"}"
                labels_str = '{' + ','.join([f'{k}="{v}"' for k, v in labels.items()]) + '}'
                
                # Agrupa por labels
                if labels_str not in streams:
                    streams[labels_str] = []
                    
                # Adiciona entrada no stream
                streams[labels_str].append([str(ts), line])
            
            # Formato do payload Loki
            payload = {"streams": [{"stream": {"app": "log_capturer", **eval(f"dict({k[1:-1]})")}, 
                                  "values": v} for k, v in streams.items()]}
            
            # Envia para o Loki
            session = await http_session_pool.get_session()
            try:
                start_time = time.time()
                
                # NOVO: Implementar suporte a compressão HTTP
                headers = {}
                
                # Determinar se devemos usar compressão
                use_compression = SINK_HTTP_COMPRESSION_ENABLED
                
                # Verificar tamanho do payload para compressão
                payload_json = json.dumps(payload)
                payload_bytes = payload_json.encode('utf-8')
                payload_size = len(payload_bytes)
                
                # Aplicar compressão se habilitada e payload grande o suficiente
                if use_compression and payload_size >= SINK_HTTP_COMPRESSION_MIN_BYTES:
                    compressed_payload = None
                    compression_algo = SINK_HTTP_COMPRESSION_ALGO
                    compression_level = SINK_HTTP_COMPRESSION_LEVEL
                    
                    # Compressão adaptativa - ajustar nível conforme tamanho
                    if SINK_HTTP_COMPRESSION_ADAPTIVE:
                        # Payload pequeno = compressão mais leve, payload grande = mais pesada
                        if payload_size < 50 * 1024:  # < 50KB
                            compression_level = max(1, compression_level - 2)
                        elif payload_size > 500 * 1024:  # > 500KB
                            compression_level = min(9, compression_level + 1)
                    
                    # Aplicar algoritmo de compressão selecionado
                    if compression_algo == 'gzip':
                        import gzip
                        compressed_payload = gzip.compress(payload_bytes, compresslevel=compression_level)
                        headers['Content-Encoding'] = 'gzip'
                    
                    elif compression_algo == 'zstd':
                        try:
                            import zstd
                            compressed_payload = zstd.compress(payload_bytes, compression_level)
                            headers['Content-Encoding'] = 'zstd'
                        except ImportError:
                            # Fallback para gzip se zstd não disponível
                            import gzip
                            compressed_payload = gzip.compress(payload_bytes, compresslevel=compression_level)
                            headers['Content-Encoding'] = 'gzip'
                    
                    elif compression_algo == 'br':
                        try:
                            import brotli
                            compressed_payload = brotli.compress(payload_bytes, quality=compression_level)
                            headers['Content-Encoding'] = 'br'
                        except ImportError:
                            # Fallback para gzip se brotli não disponível
                            import gzip
                            compressed_payload = gzip.compress(payload_bytes, compresslevel=compression_level)
                            headers['Content-Encoding'] = 'gzip'
                    
                    # Se conseguimos comprimir, usar o payload comprimido
                    if compressed_payload:
                        payload_data = compressed_payload
                        headers['Content-Type'] = 'application/json'
                        
                        # Log da compressão aplicada
                        compression_ratio = round((1 - len(compressed_payload) / payload_size) * 100, 1)
                        self.logger.debug(
                            "Compressão HTTP aplicada", 
                            algorithm=compression_algo,
                            level=compression_level,
                            original_size=payload_size,
                            compressed_size=len(compressed_payload),
                            ratio_percent=f"{compression_ratio}%"
                        )
                    else:
                        # Fallback para não comprimido
                        payload_data = payload
                        
                else:
                    # Sem compressão - usar payload normal
                    payload_data = payload
                
                # Fazer a requisição com ou sem compressão conforme determinado acima
                async with session.post(self.url, json=payload if not headers else None,
                                        data=payload_data if headers else None,
                                        headers=headers) as resp:
                    response_text = await resp.text()
                    success = 200 <= resp.status < 300
                    if success:
                        # feedback para rate limiter / métricas se necessário
                        return True

                    # Erro não-success: checar caso de timestamp 'too far behind'
                    self.logger.error("Erro do Loki", status=resp.status, response=response_text[:500])
                    if resp.status == 400 and "entry too far behind" in (response_text or ""):
                        # Caso: timestamps antigos rejeitados pelo Loki
                        if TIMESTAMP_CLAMP_ENABLED:
                            # Tentar clamped resend: ajustar timestamps para agora e reenviar uma vez
                            try:
                                self.logger.info("Tentando reenviar batch com timestamps ajustados (clamp)", batch_size=len(batch))
                                now_ns = int(time.time() * 1e9)
                                adjusted = []
                                for rec in batch:
                                    new_rec = dict(rec)
                                    new_rec['ts'] = now_ns
                                    adjusted.append(new_rec)
                                # Rebuild payload e reenvia (sem headers alterados)
                                adj_streams = {}
                                for record in adjusted:
                                    ts = record.get('ts', int(time.time() * 1e9))
                                    line = record.get('line', '')
                                    labels = record.get('labels', {})
                                    labels_str = '{' + ','.join([f'{k}="{v}"' for k, v in labels.items()]) + '}'
                                    adj_streams.setdefault(labels_str, []).append([str(ts), line])
                                adj_payload = {"streams": [{"stream": {"app": "log_capturer", **eval(f"dict({k[1:-1]})")}, "values": v} for k, v in adj_streams.items()]}
                                adj_payload_json = json.dumps(adj_payload)
                                adj_payload_bytes = adj_payload_json.encode('utf-8')
                                # aplicar mesma compressão se necessário
                                adj_payload_data = adj_payload_bytes
                                if headers.get('Content-Encoding'):
                                    # recomprimir conforme algoritmo (fallback gzip se necessário)
                                    try:
                                        if headers['Content-Encoding'] == 'gzip':
                                            import gzip
                                            adj_payload_data = gzip.compress(adj_payload_bytes, compresslevel=SINK_HTTP_COMPRESSION_LEVEL)
                                        elif headers['Content-Encoding'] in ('zstd','zst'):
                                            import zstd as _z
                                            adj_payload_data = _z.compress(adj_payload_bytes, SINK_HTTP_COMPRESSION_LEVEL)
                                    except Exception:
                                        adj_payload_data = adj_payload_bytes

                                async with session.post(self.url, json=adj_payload if not headers else None,
                                                        data=adj_payload_data if headers else None,
                                                        headers=headers) as resp2:
                                    text2 = await resp2.text()
                                    if 200 <= resp2.status < 300:
                                        self.logger.info("Reenvio com clamp bem-sucedido", batch_size=len(batch))
                                        return True
                                    else:
                                        self.logger.error("Reenvio com clamp falhou", status=resp2.status, response=text2[:500])
                                        # Ainda falhando: mover para DLQ se configurado
                                        if TIMESTAMP_CLAMP_DLQ:
                                            await self._send_to_dlq(batch, "timestamp_rejected_after_clamp")
                                            return True
                                        return False
                            except Exception as e:
                                self.logger.error("Erro ao tentar reenvio com clamp", error=str(e))
                                if TIMESTAMP_CLAMP_DLQ:
                                    await self._send_to_dlq(batch, "timestamp_rejected_after_clamp_error")
                                    return True
                                return False
                        else:
                            # Clamp desabilitado: mover para DLQ se policy exigir, caso contrário falha
                            self.logger.warning("Timestamp rejeitado pelo Loki e clamp desabilitado", batch_size=len(batch))
                            if TIMESTAMP_CLAMP_DLQ:
                                await self._send_to_dlq(batch, "timestamp_rejected")
                                return True
                            return False

                    # Outros erros HTTP: considerar retry no worker/persistência
                    return False
            except Exception as e:
                self.logger.error("Exceção ao enviar para Loki", error=str(e))
                return False
            finally:
                await http_session_pool.return_session(session)
        except Exception as e:
            # Captura o stack trace completo
            self.logger.error("Erro ao preparar payload para Loki", 
                             error=str(e),
                             traceback=traceback.format_exc())
            return False

    async def _worker_loop(self):
        """Loop principal do worker para processamento em lote com persistência opcional."""
        self.logger.info("Worker loop do LokiSink iniciado", persistence_enabled=self._persistence_enabled)
        consecutive_failures = 0
        max_consecutive_failures = int(SINK_PERSISTENCE_RECOVERY_MAX_RETRIES or 10)
        persisted_retry_backoff = float(SINK_PERSISTENCE_RECOVERY_BACKOFF_BASE or 1.0)
        persisted_retry_backoff_max = float(SINK_PERSISTENCE_RECOVERY_BACKOFF_MAX or 30.0)
        while not self._shutdown_event.is_set():
            try:
                batch = []
                from_disk = False

                # Removido: não carregar batch persistido aqui, apenas processar da fila
                # O recovery de batches persistidos é feito exclusivamente pela tarefa _recovery_loop

                # coleta de logs da fila (não bloqueante)
                consecutive_failures = 0
                start_wait = time.time()
                batch_size = self._batcher.get_optimal_size()
                while (len(batch) < batch_size and 
                      time.time() - start_wait < self.batch_timeout and
                      not self._shutdown_event.is_set()):
                    try:
                        item = await asyncio.wait_for(
                            self.queue.get(), 
                            timeout=max(0.1, self.batch_timeout - (time.time() - start_wait))
                        )
                        batch.append(item)
                    except asyncio.TimeoutError:
                        break
                if batch and self._persistence_enabled:
                    await self._persist_batch(batch)

                if not batch:
                    try:
                        item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                        batch.append(item)
                        if self._persistence_enabled:
                            await self._persist_batch(batch)
                    except asyncio.TimeoutError:
                        await asyncio.sleep(0.1)
                        # Adiciona heartbeat mesmo em ciclos sem batch
                        await self._send_heartbeat()
                        continue

                if batch:
                    try:
                        batch_size = len(batch)
                        success = await self.send_batch(batch)
                        self._stats['batches_processed'] += 1
                        if success:
                            await self._remove_persistence()
                            self._stats['logs_sent'] += len(batch)
                            consecutive_failures = 0
                            persisted_retry_backoff = float(SINK_PERSISTENCE_RECOVERY_BACKOFF_BASE or 1.0)
                        else:
                            if not from_disk:
                                await self._optimized_overflow_to_disk(batch)
                            self._stats['batches_failed'] += 1
                            if from_disk:
                                consecutive_failures += 1
                                try:
                                    await asyncio.sleep(min(persisted_retry_backoff, persisted_retry_backoff_max))
                                except Exception:
                                    pass
                                persisted_retry_backoff = min(persisted_retry_backoff * 2.0, persisted_retry_backoff_max)
                    except Exception as e:
                        self.logger.error("Erro ao processar batch", 
                                        error=str(e), 
                                        traceback=traceback.format_exc(),
                                        batch_size=len(batch),
                                        from_disk=from_disk)
                        if not from_disk:
                            await self._optimized_overflow_to_disk(batch)
                        else:
                            consecutive_failures += 1
                        self._stats['batches_failed'] += 1
                # Adiciona heartbeat ao final de cada ciclo do loop
                await self._send_heartbeat()
            except asyncio.CancelledError:
                self.logger.info("Worker loop do LokiSink cancelado")
                break
            except Exception as e:
                self.logger.error("Erro crítico no worker loop do LokiSink", 
                                error=str(e),
                                traceback=traceback.format_exc())
                await asyncio.sleep(1)
        self.logger.info("Worker loop do LokiSink finalizado", stats=self._stats)

class ElasticsearchSink(AbstractSink):
    def __init__(self, queue_size: int, hosts: List[str], index_prefix: str, buffer_dir):
        super().__init__("elasticsearch", queue_size, buffer_dir)
        self.hosts = hosts
        self.index_prefix = index_prefix
        self.batch_size = 100
        self.client = None

    async def start(self):
        try:
            from elasticsearch import AsyncElasticsearch  # import lazy
        except ImportError:
            self.logger.error("Elasticsearch sink requires elasticsearch library")
            raise
        try:
            self.client = AsyncElasticsearch(self.hosts)
        except Exception as e:
            self.logger.error("Falha ao criar cliente Elasticsearch", error=str(e))
            raise
        await super().start()
        self.logger.debug("ElasticsearchSink start concluído")

    async def stop(self):
        if self.client:
            try:
                await self.client.close()
            except Exception:
                pass
        await super().stop()

    async def send_batch(self, batch: List[Dict]) -> bool:
        self.logger.debug("Enviando batch para Elasticsearch", batch_size=len(batch))
        try:
            from elasticsearch.helpers import async_bulk  # import lazy
            index_name = f"{self.index_prefix}-{datetime.utcnow().strftime('%Y.%m.%d')}"
            actions = []
            for log in batch:
                actions.append({
                    "_index": index_name,
                    "_source": {
                        "@timestamp": datetime.utcfromtimestamp(log['ts'] / 1e9).isoformat() + 'Z',
                        "message": log['line'],
                        "labels": log['labels'],
                        "meta": log.get('meta', {})
                    }
                })
            success, _ = await async_bulk(self.client, actions)
            return success == len(actions)
        except Exception:
            self._track_error()
            return False

    async def _worker_loop(self):
        """Loop principal do worker para processamento em lote com persistência opcional."""
        self.logger.info("Worker loop do ElasticsearchSink iniciado", persistence_enabled=self._persistence_enabled)
        
        while not self._shutdown_event.is_set():
            try:
                batch = []
                from_disk = False
                
                # Verifica se há batch persistido para recuperação
                if self._persistence_enabled:
                    persisted_batch = await self._load_persisted_batch()
                    if persisted_batch:
                        batch = persisted_batch
                        from_disk = True
                
                # Se não há batch persistido, coleta da fila
                if not batch:
                    start_wait = time.time()
                    try:
                        item = await asyncio.wait_for(self.queue.get(), timeout=5.0)
                        batch.append(item)
                        
                        # Coleta mais itens disponíveis até o limite do batch
                        while len(batch) < self.batch_size and not self.queue.empty():
                            try:
                                batch.append(self.queue.get_nowait())
                            except asyncio.QueueEmpty:
                                break
                        
                        # Persiste o novo batch se a persistência estiver ativada
                        if batch and self._persistence_enabled:
                            await self._persist_batch(batch)
                            
                    except asyncio.TimeoutError:
                        # Timeout, continua loop
                        continue
                
                # Processa o batch
                if batch:
                    try:
                        success = await self.send_batch(batch)
                        self._stats['batches_processed'] += 1
                        
                        if success:
                            # Sucesso: remove persistência e atualiza contadores
                            await self._remove_persistence()
                            self._stats['logs_sent'] += len(batch)
                        else:
                            # Falha: tenta buffer em disco
                            if not from_disk:
                                await self._optimized_overflow_to_disk(batch)
                            self._stats['batches_failed'] += 1
                            
                    except Exception as e:
                        self.logger.error("Erro ao processar batch", 
                                         error=str(e), 
                                         batch_size=len(batch))
                        
                        if not from_disk:
                            await self._optimized_overflow_to_disk(batch)
                        self._stats['batches_failed'] += 1
                        
            except asyncio.CancelledError:
                self.logger.info("Worker loop do ElasticsearchSink cancelado")
                break
            except Exception as e:
                self.logger.error("Erro no worker loop do ElasticsearchSink", error=str(e))
                await asyncio.sleep(1)
        
        self.logger.info("Worker loop do ElasticsearchSink finalizado", stats=self._stats)

# --- LocalFileSink ---
class LocalFileSink(AbstractSink):
    def __init__(self, queue_size: int):
        local_buffer_dir = BASE_DIR / "local_buffer"
        local_buffer_dir.mkdir(parents=True, exist_ok=True)
        # Limite do buffer local agora configurável (aciona limpeza unificada e cota)
        super().__init__("localfile", queue_size, local_buffer_dir, max_buffer_size_mb=LOCAL_SINK_MAX_SIZE_MB)
        self.batch_size = LOCAL_SINK_BATCH_SIZE
        self.max_file_size_bytes = int(LOCAL_SINK_MAX_FILE_SIZE_MB) * 1024 * 1024

    async def _maybe_rotate_file(self, file_path: str, pending_bytes: int = 0):
        """
        Se o arquivo alvo exceder o limite (tamanho atual + bytes pendentes), faz rotação
        e comprime o rotacionado de forma assíncrona (zstd|lz4|gzip conforme config).
        """
        try:
            p = Path(file_path)
            if self.max_file_size_bytes <= 0 or not p.exists():
                return
            current_size = p.stat().st_size
            if current_size + max(0, pending_bytes) <= self.max_file_size_bytes:
                return
            ts = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
            base = p.name
            dirp = p.parent
            idx = 0
            rotated = dirp / f"{base}.{ts}.{idx}"
            while rotated.exists():
                idx += 1
                rotated = dirp / f"{base}.{ts}.{idx}"
            os.rename(str(p), str(rotated))
            self.logger.info("Arquivo local rotacionado", original=file_path, rotated=str(rotated), size_bytes=current_size)
            ext = ".zst" if DISK_BUFFER_COMPRESSION_ALGO in ("zstd", "zst") else (".lz4" if DISK_BUFFER_COMPRESSION_ALGO == "lz4" else ".gz")
            dst = rotated.with_suffix(rotated.suffix + ext)
            def _compress_sync(src: Path, dst: Path):
                algo = DISK_BUFFER_COMPRESSION_ALGO
                level = int(DISK_BUFFER_COMPRESSION_LEVEL)
                try:
                    with open(src, 'rb') as fin:
                        if algo == 'zstd' or algo == 'zst':
                            import zstd
                            compressed = zstd.compress(fin.read(), level=level)
                            with open(dst, 'wb') as fout:
                                fout.write(compressed)
                        elif algo == 'lz4':
                            import lz4.frame
                            with lz4.frame.open(dst, 'wb', compression_level=level) as fout:
                                fout.write(fin.read())
                        else:
                            import gzip
                            with gzip.open(dst, 'wb', compresslevel=level) as fout:
                                fout.write(fin.read())
                    # Remove original se compressão sucedeu
                    if dst.exists() and dst.stat().st_size > 0:
                        src.unlink()
                except Exception as e:
                    self.logger.error(f"Erro ao comprimir arquivo: {str(e)}")
            try:
                asyncio.create_task(asyncio.to_thread(_compress_sync, rotated, dst))
            except Exception:
                pass
        except Exception as e:
            self.logger.error("Falha na rotação de arquivo", file=file_path, error=str(e))

    async def send_batch(self, batch: List[Dict]) -> bool:
        self.logger.debug("Enviando batch para arquivo local", batch_size=len(batch))
        try:
            files_data: Dict[str, List[str]] = {}
            for log_entry in batch:
                meta = log_entry.get('meta', {})
                is_dlq = meta.get('is_dlq', False)
                source_type = meta.get('source_type', 'unknown')
                source_id = meta.get('source_id', 'unknown')

                # diretório/arquivo de destino
                dir_path = BASE_DIR / ('dlq' if is_dlq else source_type)
                filename = f"{source_id}.dlq.log" if is_dlq else f"{source_id}.log"
                file_path = str(dir_path / filename)

                files_data.setdefault(file_path, []).append(json.dumps(log_entry) + "\n")

            ok_count = 0
            for file_path, lines in files_data.items():
                try:
                    await self._maybe_rotate_file(file_path, pending_bytes=sum(len(x.encode("utf-8")) for x in lines))
                except Exception:
                    pass
                if await optimized_file_executor.write_batch_optimized(file_path, lines):
                    ok_count += 1

            return ok_count == len(files_data)
        except Exception:
            self._track_error()
            return False

    async def _worker_loop(self):
        """Loop principal do worker para processamento em lote com persistência opcional."""
        self.logger.info("Worker loop do LocalFileSink iniciado", persistence_enabled=self._persistence_enabled)
        
        while not self._shutdown_event.is_set():
            try:
                batch = []
                from_disk = False
                
                # Verifica se há batch persistido para recuperação
                if self._persistence_enabled:
                    persisted_batch = await self._load_persisted_batch()
                    if persisted_batch:
                        batch = persisted_batch
                        from_disk = True
                
                # Se não há batch persistido, coleta da fila
                if not batch:
                    try:
                        # Espera por um item e depois coleta mais até o batch_size
                        item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                        batch.append(item)
                        
                        # Tenta esvaziar a fila até o batch_size
                        for _ in range(min(self.batch_size - 1, self.queue.qsize())):
                            try:
                                batch.append(self.queue.get_nowait())
                            except asyncio.QueueEmpty:
                                break
                        
                        # Persiste o batch se necessário
                        if self._persistence_enabled and batch:
                            await self._persist_batch(batch)
                    except asyncio.TimeoutError:
                        await asyncio.sleep(0.1)
                        # Adiciona heartbeat mesmo em ciclos sem batch
                        await self._send_heartbeat()
                        continue
                
                # Processa o batch
                if batch:
                    try:
                        success = await self.send_batch(batch)
                        self._stats['batches_processed'] += 1
                        
                        if success:
                            # Sucesso: remove persistência e atualiza contadores
                            await self._remove_persistence()
                            self._stats['logs_written'] += len(batch)
                        else:
                            # Falha: tenta buffer em disco
                            if not from_disk:
                                await self._optimized_overflow_to_disk(batch)
                            self._stats['batches_failed'] += 1
                    except Exception as e:
                        self.logger.error("Erro ao processar batch", 
                                        error=str(e), 
                                        batch_size=len(batch))
                        if not from_disk:
                            await self._optimized_overflow_to_disk(batch)
                        self._stats['batches_failed'] += 1
                # Adiciona heartbeat ao final de cada ciclo do loop
                await self._send_heartbeat()
            except asyncio.CancelledError:
                self.logger.info("Worker loop do LocalFileSink cancelado")
                break
            except Exception as e:
                self.logger.error("Erro no worker loop do LocalFileSink", error=str(e))
                await asyncio.sleep(1)
        self.logger.info("Worker loop do LocalFileSink finalizado", stats=self._stats)

# SplunkSink - implementação estava ausente
class SplunkSink(AbstractSink):
    def __init__(self, queue_size: int, hec_url: str, token_secret: str, buffer_dir, secret_manager):
        super().__init__("splunk", queue_size, buffer_dir)
        self.hec_url = hec_url
        self.token_secret = token_secret
        self.secret_manager = secret_manager
        self.batch_size = 100
        self._token = None

    async def start(self):
        self._token = self.secret_manager.get_secret(self.token_secret)
        if not self._token:
            raise ValueError(f"Splunk HEC token not found for secret: {self.token_secret}")
        await super().start()
        self.logger.debug("SplunkSink start concluído")

    async def send_batch(self, batch: List[Dict]) -> bool:
        self.logger.debug("Enviando batch para Splunk", batch_size=len(batch))
        if not self._token:
            self.logger.error("Token HEC não disponível")
            return False
        
        session = await http_session_pool.get_session()
        try:
            events = []
            for log in batch:
                event = {
                    "time": log['ts'] / 1e9,  # Splunk espera timestamp em segundos
                    "source": log.get('meta', {}).get('source_type', 'unknown'),
                    "sourcetype": "_json",
                    "event": {
                        "message": log['line'],
                        "labels": log['labels'],
                        "meta": log.get('meta', {})
                    }
                }
                events.append(event)
            
            headers = {
                "Authorization": f"Splunk {self._token}",
                "Content-Type": "application/json"
            }
            
            # Enviar cada evento separadamente (HEC padrão)
            for event in events:
                async with session.post(self.hec_url, json=event, headers=headers) as resp:
                    if resp.status not in (200, 201):
                        self.logger.error("Splunk rejeitou evento", status=resp.status)
                        return False
            
            return True
        except Exception as e:
            self._track_error()
            self.logger.error("Erro ao enviar para Splunk", error=str(e))
            return False
        finally:
            await http_session_pool.return_session(session)

    async def _worker_loop(self):
        """Loop principal do worker para processamento em lote com persistência opcional."""
        self.logger.info("Worker loop do SplunkSink iniciado", persistence_enabled=self._persistence_enabled)
        
        while not self._shutdown_event.is_set():
            try:
                batch = []
                from_disk = False
                
                # NOVO: Se persistência habilitada, tentar carregar batch pendente primeiro
                if self._persistence_enabled:
                    persisted_batch = await self._load_persisted_batch()
                    if persisted_batch:
                        batch = persisted_batch
                        from_disk = True
                
                # Se não há batch persistido, coletar novos logs da fila
                if not batch:
                    # Espera até ter logs suficientes ou timeout
                    start_wait = time.time()
                    batch_size = self.batch_size  # SplunkSink não tem _batcher
                    
                    while (len(batch) < batch_size and 
                          time.time() - start_wait < 1.0 and  # Usando timeout fixo como fallback
                          not self._shutdown_event.is_set()):
                        try:
                            item = await asyncio.wait_for(
                                self.queue.get(), 
                                timeout=max(0.1, 1.0 - (time.time() - start_wait))
                            )
                            batch.append(item)
                        except asyncio.TimeoutError:
                            break
                
                # Se não há logs para processar, aguarda
                if not batch:
                    try:
                        item = await asyncio.wait_for(self.queue.get(), timeout=1.0)
                        batch.append(item)
                        if self._persistence_enabled:
                            await self._persist_batch(batch)
                    except asyncio.TimeoutError:
                        await asyncio.sleep(0.1)
                        continue
        
                # Processar o batch
                if batch:
                    try:
                        # Mede tamanho antes para métricas
                        batch_size = len(batch)
                        
                        # Tenta enviar para destino
                        success = await self.send_batch(batch)
                        
                        # Atualiza estatísticas
                        self._stats['batches_processed'] += 1
                        if success:
                            await self._remove_persistence()
                            self._stats['logs_sent'] += len(batch)
                        else:
                            if not from_disk:
                                await self._optimized_overflow_to_disk(batch)
                            self._stats['batches_failed'] += 1
            
                    except Exception as e:
                        self.logger.error("Erro ao processar batch", 
                                        error=str(e), 
                                        traceback=traceback.format_exc(),
                                        batch_size=len(batch),
                                        from_disk=from_disk)
                
                        # Overflow para disco se não for do disco
                        if not from_disk:
                            await self._optimized_overflow_to_disk(batch)
                        
                        # Atualiza estatísticas de falha
                        self._stats['batches_failed'] += 1
            
            except asyncio.CancelledError:
                self.logger.info("Worker loop do SplunkSink cancelado")
                break
            except Exception as e:
                self.logger.error("Erro crítico no worker loop do SplunkSink", 
                                error=str(e),
                                traceback=traceback.format_exc())
                await asyncio.sleep(1)  # Evitar loop tight em caso de erro
        
        self.logger.info("Worker loop do SplunkSink finalizado", stats=self._stats)