import os
import time
import asyncio
import aiofiles
import aiohttp
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from .config import (
    HTTP_SESSION_POOL_SIZE, HTTP_CONNECTION_TIMEOUT, HTTP_REQUEST_TIMEOUT,
    HTTP_CONNECTOR_LIMIT, HTTP_CONNECTOR_LIMIT_PER_HOST, FILE_IO_THREAD_POOL_SIZE,
    CLEANUP_INTERVAL_SECONDS, CLEANUP_AGE_THRESHOLD_SECONDS, ADAPTIVE_THROTTLING_ENABLED,
    THROTTLE_SLEEP_BASE, THROTTLE_SLEEP_MAX, BACKPRESSURE_THRESHOLD,
    SINK_RATE_LIMIT_ENABLED, SINK_RATE_LIMIT_RPS, SINK_RATE_LIMIT_BURST,
    SINK_RATE_LIMIT_MIN_RPS, SINK_RATE_LIMIT_MAX_RPS,
    SINK_RATE_LIMIT_LATENCY_TARGET_MS,
)
from .metrics import metrics

class HTTPConnectionPool:
    def __init__(self, pool_size=HTTP_SESSION_POOL_SIZE, conn_timeout=HTTP_CONNECTION_TIMEOUT, req_timeout=HTTP_REQUEST_TIMEOUT, limit=HTTP_CONNECTOR_LIMIT, limit_per_host=HTTP_CONNECTOR_LIMIT_PER_HOST):
        self.pool_size = pool_size
        self.conn_timeout = conn_timeout
        self.req_timeout = req_timeout
        self.limit = limit
        self.limit_per_host = limit_per_host
        self.available_sessions = asyncio.Queue(maxsize=pool_size)
        self.sessions = []
        self.initialized = False
    async def initialize(self):
        timeout = aiohttp.ClientTimeout(total=self.req_timeout, connect=self.conn_timeout)
        connector = aiohttp.TCPConnector(limit=self.limit, limit_per_host=self.limit_per_host)
        for _ in range(self.pool_size):
            session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            self.sessions.append(session)
            await self.available_sessions.put(session)
        self.initialized = True
    async def get_session(self):
        if not self.initialized:
            await self.initialize()
        s = await self.available_sessions.get()
        metrics.HTTP_SESSION_POOL_AVAILABLE.set(self.available_sessions.qsize())
        metrics.HTTP_SESSION_POOL_ACTIVE.set(self.pool_size - self.available_sessions.qsize())
        return s
    async def return_session(self, session):
        await self.available_sessions.put(session)
        metrics.HTTP_SESSION_POOL_AVAILABLE.set(self.available_sessions.qsize())
        metrics.HTTP_SESSION_POOL_ACTIVE.set(self.pool_size - self.available_sessions.qsize())
    async def close(self):
        # Fecha todas as sessões abertas e drena a fila
        try:
            while not self.available_sessions.empty():
                try:
                    await self.available_sessions.get()
                except Exception:
                    break
            for s in self.sessions:
                try:
                    await s.close()
                except Exception:
                    pass
        finally:
            self.sessions.clear()
            self.initialized = False
            # NOVO: zera métricas do pool ao encerrar
            try:
                metrics.HTTP_SESSION_POOL_AVAILABLE.set(0)
                metrics.HTTP_SESSION_POOL_ACTIVE.set(0)
            except Exception:
                pass

class OptimizedFileExecutor:
    def __init__(self, max_workers=FILE_IO_THREAD_POOL_SIZE):
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
    async def write_batch_optimized(self, file_path: str, lines: List[str]) -> bool:
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        metrics.FILE_IO_PENDING_WRITES.labels(file_path=file_path).inc()
        try:
            async with aiofiles.open(file_path, 'a', encoding='utf-8') as f:
                await f.writelines(lines)
            return True
        except Exception:
            return False
        finally:
            metrics.FILE_IO_PENDING_WRITES.labels(file_path=file_path).dec()
    async def stop(self):
        self.executor.shutdown(wait=True)

class UnifiedCleanupManager:
    def __init__(self):
        self.cleanup_targets = []
        self.stats = {"files_removed": 0, "bytes_freed": 0}
        self.task = None
    def register_cleanup_target(self, directory: Path, max_size_mb: int, patterns: List[str]):
        self.cleanup_targets.append({"directory": directory, "max_bytes": max_size_mb*1024*1024, "patterns": patterns})
    async def start(self):
        self.task = asyncio.create_task(self._cleanup_loop())
    async def stop(self):
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
    async def _cleanup_loop(self):
        while True:
            try:
                await self._cleanup_all_targets()
                await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(60)
    async def _cleanup_all_targets(self):
        for target in list(self.cleanup_targets):
            await self._cleanup_target(target)
    async def _cleanup_target(self, target: dict):
        directory = target["directory"]
        max_bytes = target["max_bytes"]
        now = time.time()
        all_files = []
        for pattern in target["patterns"]:
            all_files.extend([f for f in directory.glob(pattern) if f.is_file()])
        bytes_freed = 0
        removed = 0
        remaining_files = []
        for f in all_files:
            try:
                st = f.stat()
                if now - st.st_mtime > CLEANUP_AGE_THRESHOLD_SECONDS:
                    size = st.st_size
                    f.unlink()
                    bytes_freed += size
                    removed += 1
                else:
                    remaining_files.append((f, st))
            except Exception:
                pass
        total = sum(st.st_size for _, st in remaining_files)
        if total > max_bytes:
            remaining_files.sort(key=lambda p: p[1].st_mtime)
            for f, st in remaining_files:
                if total <= max_bytes: break
                try:
                    size = st.st_size
                    f.unlink()
                    total -= size
                    bytes_freed += size
                    removed += 1
                except Exception:
                    pass
        if removed:
            self.stats["files_removed"] += removed
            self.stats["bytes_freed"] += bytes_freed

class AdaptiveThrottler:
    def __init__(self, name: str, enabled=ADAPTIVE_THROTTLING_ENABLED, base_delay=THROTTLE_SLEEP_BASE, max_delay=THROTTLE_SLEEP_MAX):
        self.name = name
        self.enabled = enabled
        self.current_delay = 0.0
        self.base_delay_increment = base_delay
        self.max_delay = max_delay
    async def throttle(self, queue_load: float, recent_errors: int):
        if not self.enabled:
            return
        if queue_load > BACKPRESSURE_THRESHOLD or recent_errors > 0:
            self.current_delay = min(self.current_delay + self.base_delay_increment, self.max_delay)
            await asyncio.sleep(self.current_delay)
        else:
            self.current_delay = max(self.current_delay - (self.base_delay_increment/2), 0.0)

class AdaptiveRateLimiter:
    """
    Token-bucket adaptativo por feedback (429/5xx/latência).
    - tokens/s = rps efetivo (ajustável).
    - acquire(cost): consome 'cost' tokens (custo proporcional ao payload).
    - on_feedback(): aplica backoff/recovery.
    """
    def __init__(self,
                 enabled: bool = SINK_RATE_LIMIT_ENABLED,
                 base_rps: float = SINK_RATE_LIMIT_RPS,
                 burst: float = SINK_RATE_LIMIT_BURST,
                 min_rps: float = SINK_RATE_LIMIT_MIN_RPS,
                 max_rps: float = SINK_RATE_LIMIT_MAX_RPS,
                 latency_target_ms: float = SINK_RATE_LIMIT_LATENCY_TARGET_MS):
        self.enabled = enabled
        self._rps = max(min_rps, min(base_rps, max_rps))
        self._burst = max(burst, self._rps)
        self._min_rps = max(0.1, min_rps)
        self._max_rps = max(self._rps, max_rps)
        self._tokens = self._burst
        self._last = time.time()
        self._lock = asyncio.Lock()
        self._latency_target_ms = latency_target_ms
        self._backoff_factor = 0.8
        self._recover_factor = 1.05
        self._logger = None  # opcional: setado pelo sink

    def set_logger(self, logger):
        self._logger = logger

    async def acquire(self, cost: float = 1.0) -> float:
        if not self.enabled:
            return 0.0
        start = time.time()
        async with self._lock:
            while self._tokens < cost:
                now = time.time()
                elapsed = max(0.0, now - self._last)
                self._last = now
                self._tokens = min(self._burst, self._tokens + elapsed * self._rps)
                if self._tokens >= cost:
                    break
                sleep_time = (cost - self._tokens) / max(self._rps, 0.1)
                await asyncio.sleep(min(sleep_time, 1.0))
            self._tokens -= cost
        delay = time.time() - start
        if delay > 0 and self._logger:
            try:
                self._logger.debug("RateLimiter aplicou espera", waited_ms=round(delay * 1000, 2), rps=round(self._rps, 2), cost=round(cost, 2))
            except Exception:
                pass
        return delay

    def on_feedback(self, success: bool, http_status: int, latency_ms: float):
        if not self.enabled:
            return
        adjust = None
        # backoff em 429/5xx ou latência alta
        if (not success) or (http_status in (429,)) or (500 <= http_status < 600) or (latency_ms > self._latency_target_ms * 1.3):
            new_rps = max(self._min_rps, self._rps * self._backoff_factor)
            adjust = ("down", new_rps)
            self._rps = new_rps
        # recuperação lenta quando ok e latência baixa
        elif success and latency_ms < self._latency_target_ms * 0.8:
            new_rps = min(self._max_rps, self._rps * self._recover_factor)
            if new_rps != self._rps:
                adjust = ("up", new_rps)
                self._rps = new_rps
        if adjust and self._logger:
            direction, val = adjust
            try:
                self._logger.info("RateLimiter ajuste de RPS", direction=direction, rps=round(val, 2))
            except Exception:
                pass

# Instâncias globais
http_session_pool = HTTPConnectionPool()
optimized_file_executor = OptimizedFileExecutor()
unified_cleanup_manager = UnifiedCleanupManager()
adaptive_rate_limiter = AdaptiveRateLimiter()
