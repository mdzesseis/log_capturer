import os
import json
import time
import aiofiles
import hashlib
import gzip
import shutil
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
            files = []
            for f in self.directory.rglob('*.jsonl'):
                if f.is_file():
                    st = f.stat()
                    files.append((st.st_mtime, f, st.st_size))
            files.sort()
            for _, f, s in files:
                if freed >= to_free:
                    break
                try:
                    f.unlink()
                    freed += s
                except Exception as e:
                    self.logger.error("Erro ao remover arquivo por cota", file=str(f), error=str(e))
            if freed:
                metrics.CLEANUP_BYTES_FREED.inc(freed)
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
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        pass

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
                    import zstandard as zstd
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
            import gzip as _gzip
            with open(src, "rb") as fin, _gzip.open(dst, "wb", compresslevel=int(level if level else 6)) as fout:
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
        import asyncio
        while True:
            try:
                self.logger.debug("Atualizando métrica de buffer em disco", name=self.name)
                await self._send_heartbeat()
                size = 0
                for pattern in ('*.jsonl', '*.jsonl.gz', '*.jsonl.zst', '*.jsonl.lz4'):
                    size += sum(f.stat().st_size for f in self.buffer_dir.glob(pattern) if f.is_file())
                metrics.SINK_DISK_BUFFER_BYTES.labels(sink_name=self.name).set(size)
            except Exception:
                self.logger.debug("Erro ao atualizar métrica de buffer em disco", name=self.name)
            await asyncio.sleep(30)

    async def send_batch(self, batch: List[Dict]) -> bool:
        self.logger.debug("Enviando batch para destino", name=self.name, batch_size=len(batch))
        raise NotImplementedError

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

    async def start(self):
        await super().start()
        self.logger.debug("LokiSink start concluído")

    async def _worker_loop(self):
        import asyncio
        self.logger.debug("LokiSink worker loop iniciado")
        while True:
            try:
                await self._send_heartbeat()
                batch = []
                # NOVO: fairness - se a fila em memória estiver alta, priorizar drenagem da fila
                qsize = self.queue.qsize()
                qcap = max(1, self.queue.maxsize or 1)
                qload = qsize / qcap
                disk_data = None
                if qload < 0.5:
                    disk_data = await self._read_from_disk_buffer()
                else:
                    self.logger.info("Pulando leitura do buffer em disco por alta carga na fila", queue_size=qsize, queue_capacity=qcap, queue_load=round(qload, 3))
                self.logger.debug("Batch lido do disco", from_disk=bool(disk_data), batch_size=len(disk_data["batch"]) if disk_data else 0)

                if disk_data:
                    batch = disk_data["batch"]
                    file_path = disk_data["file_path"]
                    restore_path = disk_data.get("restore_path")
                    from_disk = True
                else:
                    try:
                        target_size = self._batcher.get_optimal_size()
                        # NOVO: deadline de montagem do batch para reduzir latência
                        assemble_deadline = time.monotonic() + min(1.0, max(0.1, self.batch_timeout / 3.0))
                        deadline_hit = False
                        while len(batch) < target_size:
                            remaining = assemble_deadline - time.monotonic()
                            if remaining <= 0:
                                deadline_hit = True
                                break
                            try:
                                item = await asyncio.wait_for(self.queue.get(), timeout=min(0.2, remaining))
                                batch.append(item)
                            except (asyncio.TimeoutError, asyncio.QueueEmpty):
                                # Se não chegou item a tempo e já temos algum batch, enviar assim mesmo
                                if batch:
                                    deadline_hit = True
                                    break
                                # Sem itens: continuar pequeno sleep para não busy-loopar
                                await asyncio.sleep(0.05)
                        if batch:
                            self.logger.debug("Batch montado a partir da fila",
                                              target_size=target_size,
                                              actual_size=len(batch),
                                              deadline_hit=deadline_hit)
                    except (asyncio.TimeoutError, asyncio.QueueEmpty):
                        if not batch:
                            await asyncio.sleep(1); continue
                    from_disk = False

                if batch:
                    self.logger.debug("Enviando batch para Loki", batch_size=len(batch))
                    try:
                        success = await self.circuit_breaker.call(self._send_batch_protected, batch)
                        state_val = 0 if self.circuit_breaker.state == CircuitBreakerState.CLOSED else (1 if self.circuit_breaker.state == CircuitBreakerState.OPEN else 2)
                        metrics.CIRCUIT_BREAKER_STATE.labels(breaker_name=self.circuit_breaker.name).set(state_val)
                        if from_disk and success:
                            try:
                                os.unlink(file_path)
                            except Exception:
                                pass
                        elif not success and not from_disk:
                            await self._optimized_overflow_to_disk(batch)
                    except Exception:
                        self.logger.debug("Erro ao enviar batch para Loki", exc_info=True)
                        self._track_error()
                        if from_disk:
                            try:
                                if restore_path:
                                    os.rename(file_path, restore_path)
                                else:
                                    # fallback: tentativa antiga para .jsonl
                                    os.rename(file_path, file_path.replace('.processing', '.jsonl'))
                            except Exception:
                                pass
                        else:
                            await self._optimized_overflow_to_disk(batch)
                await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                self.logger.debug("LokiSink worker cancelado")
                break
            except Exception:
                self.logger.debug("Erro no worker do LokiSink", exc_info=True)
                self._track_error()
                await asyncio.sleep(5)

    async def _send_batch_protected(self, batch: List[Dict]) -> bool:
        self.logger.debug("Protegendo envio de batch para Loki", batch_size=len(batch))
        return await self.send_batch(batch)

    async def send_batch(self, batch: List[Dict]) -> bool:
        self.logger.debug("Preparando envio de batch para Loki", batch_size=len(batch))
        correlation_id = CorrelationContext.get_correlation_id() or CorrelationContext.generate_correlation_id()
        CorrelationContext.set_correlation_id(correlation_id)
        session = await http_session_pool.get_session()
        try:
            streams = []
            for item in batch:
                # NOVO: evitar cópia de labels quando seguro (não modificamos labels no envio)
                labels = item['labels'] or {"source": item.get('meta',{}).get('source_type','unknown')}
                streams.append({"stream": labels, "values": [[str(item['ts']), item['line']]]})
            payload = {"streams": streams}

            # NOVO: dump único do payload (json + encode) e estimativa
            try:
                raw = json.dumps(payload).encode("utf-8")
                payload_bytes_est = len(raw)
                self.logger.debug("Payload preparado (dump único)", bytes=payload_bytes_est, batch_size=len(streams))
            except Exception:
                raw = None
                payload_bytes_est = max(1, len(streams) * 256)

            # Throttling por rate limit adaptativo proporcional ao tamanho do payload
            # NOVO: reduzir custo sob alta carga de fila para drenar backlog
            try:
                qcap = max(1, self.queue.maxsize or 1)
                qload = (self.queue.qsize() / qcap)
            except Exception:
                qload = 0.0
            eff_bytes_per_token = float(SINK_RATE_LIMIT_BYTES_PER_TOKEN)
            if qload >= 0.90:
                eff_bytes_per_token *= 8.0
            elif qload >= 0.75:
                eff_bytes_per_token *= 4.0
            elif qload >= 0.50:
                eff_bytes_per_token *= 2.0
            cost_tokens = max(1.0, payload_bytes_est / eff_bytes_per_token)
            waited = await self.rate_limiter.acquire(cost_tokens)
            if waited > 0:
                try:
                    metrics.THROTTLING_DELAY.labels(sink_name=self.name).observe(waited)
                except Exception:
                    pass
                self.logger.debug("Atraso aplicado por rate limiting", waited_ms=round(waited*1000,2), cost_tokens=round(cost_tokens,2))

            # NOVO: compressão opcional com nível adaptativo e fallback para gzip
            use_compression = SINK_HTTP_COMPRESSION_ENABLED and payload_bytes_est >= SINK_HTTP_COMPRESSION_MIN_BYTES
            headers = {}
            data_arg = None
            json_arg = None
            algo_used = None
            if use_compression and raw is not None:
                algo = (SINK_HTTP_COMPRESSION_ALGO or "gzip").lower()
                # nível base configurado
                level = int(SINK_HTTP_COMPRESSION_LEVEL)
                # NOVO: ajuste adaptativo por tamanho e CPU
                if SINK_HTTP_COMPRESSION_ADAPTIVE:
                    try:
                        import psutil
                        cpu = psutil.cpu_percent(interval=None)
                    except Exception:
                        cpu = 0.0
                    if payload_bytes_est < 128 * 1024:
                        level = 1 if cpu > 60 else 2
                    elif payload_bytes_est < 512 * 1024:
                        level = 2 if cpu > 70 else 3
                    elif payload_bytes_est < 2 * 1024 * 1024:
                        level = 3 if cpu > 80 else 4
                    else:
                        level = 3 if cpu > 85 else 4
                    self.logger.info("Compressão adaptativa definida", base=SINK_HTTP_COMPRESSION_LEVEL, effective=level, bytes=payload_bytes_est, cpu_percent=cpu)
                try:
                    if algo in ("zstd","zst"):
                        import zstandard as zstd
                        cctx = zstd.ZstdCompressor(level=level)
                        comp = cctx.compress(raw)
                        headers["Content-Encoding"] = "zstd"
                        headers["Content-Type"] = "application/json"
                        data_arg = comp
                        algo_used = "zstd"
                    else:
                        comp = gzip.compress(raw, compresslevel=level)
                        headers["Content-Encoding"] = "gzip"
                        headers["Content-Type"] = "application/json"
                        data_arg = comp
                        algo_used = "gzip"
                    self.logger.info("Compressão aplicada ao batch", algo=algo_used, original_bytes=payload_bytes_est, compressed_bytes=len(data_arg), ratio=round(len(data_arg)/payload_bytes_est, 3))
                except Exception as e:
                    self.logger.error("Falha ao comprimir payload; enviando sem compressão", error=str(e))
                    json_arg = payload
            else:
                if not SINK_HTTP_COMPRESSION_ENABLED:
                    self.logger.debug("Compressão desabilitada por configuração")
                elif payload_bytes_est < SINK_HTTP_COMPRESSION_MIN_BYTES:
                    self.logger.debug("Payload abaixo do limiar de compressão", bytes=payload_bytes_est, min_bytes=SINK_HTTP_COMPRESSION_MIN_BYTES)
                json_arg = payload

            start = time.monotonic()
            batch_size = len(streams)

            async def _req(session, payload_or_bytes, correlation_id, headers, json_mode: bool):
                if json_mode:
                    async with session.post(self.url, json=payload_or_bytes) as resp:
                        if resp.status in (200, 204):
                            return True, resp.status, ""
                        try:
                            body = await resp.text()
                        except Exception:
                            body = ""
                        return False, resp.status, body
                else:
                    async with session.post(self.url, data=payload_or_bytes, headers=headers) as resp:
                        if resp.status in (200, 204):
                            return True, resp.status, ""
                        try:
                            body = await resp.text()
                        except Exception:
                            body = ""
                        return False, resp.status, body

            try:
                ok, http_status, http_body = await self.retry_client.execute(
                    _req, session, data_arg if data_arg is not None else json_arg, correlation_id, headers, data_arg is None
                )
                # Fallback automático: se encoding não suportado, tenta gzip uma vez
                if (not ok) and use_compression and algo_used not in (None, "gzip") and http_status in (400, 415) and isinstance(http_body, str) and ("encoding" in http_body.lower() or "unsupported" in http_body.lower()):
                    try:
                        gz = gzip.compress(raw or json.dumps(payload).encode("utf-8"), compresslevel=int(SINK_HTTP_COMPRESSION_LEVEL))
                        headers2 = {"Content-Encoding": "gzip", "Content-Type": "application/json"}
                        self.logger.warning("Fallback de Content-Encoding para gzip após erro do endpoint", http_status=http_status)
                        ok, http_status, http_body = await self.retry_client.execute(_req, session, gz, correlation_id, headers2, False)
                        algo_used = "gzip"
                    except Exception:
                        pass
            except Exception as e:
                # Falha após tentativas: conta como dropped e loga erro
                self._track_error()
                metrics.PROCESSING_DROPPED.inc(batch_size)
                if not self._is_self_batch(batch):
                    self.logger.error("Falha ao enviar batch para Loki (após retry)", error=str(e), batch_size=batch_size)
                else:
                    self.logger.debug("Falha ao enviar batch para Loki (após retry) - suprimido para evitar feedback", error=str(e), batch_size=batch_size)
                # feedback ao rate limiter
                try:
                    self.rate_limiter.on_feedback(False, 599, SINK_RATE_LIMIT_LATENCY_TARGET_MS * 2)
                except Exception:
                    pass
                return False
            finally:
                # NOVO: liberar buffers grandes para reduzir pressão de GC
                try:
                    del payload
                except Exception:
                    pass

            elapsed = time.monotonic() - start
            duration_ms = elapsed * 1000.0
            try:
                metrics.SINK_SEND_LATENCY.labels(sink_name=self.name).observe(elapsed)
            except Exception:
                pass

            # Feedback ao rate limiter
            try:
                self.rate_limiter.on_feedback(ok, http_status, duration_ms)
            except Exception:
                pass

            # NOVO: tratamento de 400 "entry too far behind"
            if (not ok) and http_status == 400 and isinstance(http_body, str) and ("entry too far behind" in http_body.lower() or "too far behind" in http_body.lower()):
                # Log do motivo (como já ocorre)
                body_preview = (http_body or "")[:500]
                self.logger.warning("Lote rejeitado pelo Loki (timestamps muito antigos)",
                                    batch_size=len(batch),
                                    duration_ms=round(duration_ms, 2),
                                    url=self.url,
                                    http_status=http_status,
                                    http_body_preview=body_preview)
                if TIMESTAMP_CLAMP_DLQ:
                    await self._send_to_dlq(batch, "entry too far behind")
                else:
                    # Apenas dropa (não reprocessa)
                    try:
                        metrics.PROCESSING_DROPPED.inc(len(batch))
                    except Exception:
                        pass
                # Considera como consumido para não voltar à fila/overflow
                return True

            if ok:
                # Observa tamanho do batch e registra fila atual (consumo)
                metrics.PROCESSING_BATCH_SIZE.labels(sink_name=self.name).observe(batch_size)
                # ajuste do batch adaptativo baseado na latência observada
                try:
                    self._batcher.adjust_batch_size(elapsed)
                except Exception:
                    pass
                try:
                    metrics.SINK_QUEUE_SIZE.labels(sink_name=self.name).set(self.queue.qsize())
                except Exception:
                    pass
                if not self._is_self_batch(batch):
                    self.logger.info("Loki consumiu logs (batch enviado)", batch_size=batch_size, duration_ms=round(duration_ms, 2), url=self.url)
                else:
                    self.logger.debug("Loki consumiu logs (batch enviado) - suprimido para evitar feedback", batch_size=batch_size, duration_ms=round(duration_ms, 2), url=self.url)
                return True
            else:
                # Contabiliza como dropped e loga aviso com status/corpo
                metrics.PROCESSING_DROPPED.inc(batch_size)
                body_preview = (http_body or "")[:500]
                if not self._is_self_batch(batch):
                    self.logger.warning("Loki não confirmou consumo do batch",
                                        batch_size=batch_size,
                                        duration_ms=round(duration_ms, 2),
                                        url=self.url,
                                        http_status=http_status,
                                        http_body_preview=body_preview)
                else:
                    self.logger.debug("Loki não confirmou consumo do batch - suprimido para evitar feedback",
                                      batch_size=batch_size, duration_ms=round(duration_ms, 2),
                                      url=self.url, http_status=http_status)
                return False
        finally:
            await http_session_pool.return_session(session)

    async def _send_to_dlq(self, batch: List[Dict], reason: str):
        """Grava registros rejeitados pelo Loki em DLQ em disco com o motivo."""
        try:
            dlq_path = str(BASE_DIR / "dlq" / "loki_rejected.log")
            lines = []
            for item in batch:
                meta = dict(item.get("meta", {}))
                meta["is_dlq"] = True
                meta["dlq_reason"] = reason
                meta["dlq_sink"] = "loki"
                entry = {
                    "ts": item.get("ts"),
                    "labels": item.get("labels"),
                    "line": item.get("line"),
                    "meta": meta
                }
                lines.append(json.dumps(entry) + "\n")
                try:
                    src = item.get("meta", {}).get("source_type", "unknown")
                    metrics.LOGS_DLQ.labels(source_type=src, reason="too_far_behind").inc()
                except Exception:
                    pass
            await optimized_file_executor.write_batch_optimized(dlq_path, lines)
        except Exception:
            # Em último caso, ignora erro de DLQ para não bloquear o worker
            pass

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
        import asyncio
        while True:
            try:
                await self._send_heartbeat()
                batch = []
                try:
                    while len(batch) < self.batch_size:
                        item = await asyncio.wait_for(self.queue.get(), timeout=0.5)
                        batch.append(item)
                except (asyncio.TimeoutError, asyncio.QueueEmpty):
                    if not batch:
                        await asyncio.sleep(0.1); continue
                if batch:
                    success = await self.send_batch(batch)
                    if not success:
                        await self._optimized_overflow_to_disk(batch)
            except asyncio.CancelledError:
                break
            except Exception:
                self._track_error()
                await asyncio.sleep(5)

class SplunkSink(AbstractSink):
    def __init__(self, queue_size: int, url: str, token_secret: str, buffer_dir, secret_manager):
        super().__init__("splunk", queue_size, buffer_dir)
        self.url = url
        self.token = secret_manager.get_secret(token_secret) or ""
        self.batch_size = 100

    async def start(self):
        await super().start()
        self.logger.debug("SplunkSink start concluído")

    async def stop(self):
        await super().stop()

    async def send_batch(self, batch: List[Dict]) -> bool:
        self.logger.debug("Enviando batch para Splunk", batch_size=len(batch))
        session = await http_session_pool.get_session()
        try:
            payload = []
            for log in batch:
                payload.append({
                    "time": log['ts'] / 1e9,
                    "event": log['line'],
                    "source": log.get('meta', {}).get('source_id', 'unknown'),
                    "sourcetype": log.get('meta', {}).get('source_type', 'log'),
                    "fields": log['labels']
                })
            headers = {"Authorization": f"Splunk {self.token}"}
            async with session.post(self.url, json=payload, headers=headers) as resp:
                if resp.status == 200:
                    try:
                        result = await resp.json()
                    except Exception:
                        return False
                    return result.get('code') == 0
                return False
        except Exception:
            self._track_error()
            return False
        finally:
            await http_session_pool.return_session(session)

    async def _worker_loop(self):
        import asyncio
        while True:
            try:
                await self._send_heartbeat()
                batch = []
                try:
                    while len(batch) < self.batch_size:
                        item = await asyncio.wait_for(self.queue.get(), timeout=0.5)
                        batch.append(item)
                except (asyncio.TimeoutError, asyncio.QueueEmpty):
                    if not batch:
                        await asyncio.sleep(0.1); continue
                if batch:
                    success = await self.send_batch(batch)
                    if not success:
                        await self._optimized_overflow_to_disk(batch)
            except asyncio.CancelledError:
                break
            except Exception:
                self._track_error()
                await asyncio.sleep(5)

# --- ADICIONADO: LocalFileSink ---
class LocalFileSink(AbstractSink):
    def __init__(self, queue_size: int):
        local_buffer_dir = BASE_DIR / "local_buffer"
        local_buffer_dir.mkdir(parents=True, exist_ok=True)
        # Limite do buffer local agora configurável (aciona limpeza unificada e cota)
        super().__init__("localfile", queue_size, local_buffer_dir, max_buffer_size_mb=LOCAL_SINK_MAX_SIZE_MB)
        self.batch_size = LOCAL_SINK_BATCH_SIZE

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
                if await optimized_file_executor.write_batch_optimized(file_path, lines):
                    ok_count += 1

            return ok_count == len(files_data)
        except Exception:
            self._track_error()
            return False

    async def _worker_loop(self):
        import asyncio
        while True:
            try:
                await self._send_heartbeat()

                batch: List[Dict] = []
                disk_data = await self._read_from_disk_buffer()
                if disk_data:
                    batch = disk_data["batch"]
                    file_path = disk_data["file_path"]
                    from_disk = True
                else:
                    try:
                        while len(batch) < self.batch_size:
                            item = await asyncio.wait_for(self.queue.get(), timeout=0.1)
                            batch.append(item)
                    except (asyncio.TimeoutError, asyncio.QueueEmpty):
                        if not batch:
                            await asyncio.sleep(0.5)
                            continue
                    from_disk = False

                if batch:
                    success = await self.send_batch(batch)
                    if from_disk and success:
                        try:
                            os.unlink(file_path)
                        except Exception:
                            pass
            except asyncio.CancelledError:
                break
            except Exception:
                self._track_error()
                await asyncio.sleep(5)
