#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
metrics.py - Definição e gerenciamento de métricas para o Log Capturer.
- Métricas detalhadas para observabilidade, debug e produção.
- Facilita coleta periódica de métricas do sistema.
"""

import time
import asyncio
import psutil
from prometheus_client import Counter, Gauge, Histogram
# NOVO
import os
# NOVO: logger estruturado
from .robustness import StructuredLogger
from pathlib import Path

class Metrics:
    def __init__(self):
        # GRUPO 1: Métricas originais
        # if psutil:

    # async def start_system_metrics(interval=10):
    #     """Coleta periódica de métricas do sistema"""
    #     while True:
    #         # CPU
    #         metrics.SYSTEM_CPU.set(psutil.cpu_percent())

    #         # Memória
    #         mem = psutil.virtual_memory()
    #         metrics.SYSTEM_MEM.set(mem.used)

    #         # Disco
    #         for part in psutil.disk_partitions():
    #             usage = psutil.disk_usage(part.mountpoint)
    #             metrics.SYSTEM_DISK.labels(mountpoint=part.mountpoint).set(usage.percent)

    #         await asyncio.sleep(interval)

        self.INFO = Gauge("log_capturer_info", "Agent information", ["version"])
        self.LINES_PROCESSED = Counter("log_capturer_lines_processed_total", "Total lines processed", ["source_type","source_id","source_name"])
        self.BYTES_PROCESSED = Counter("log_capturer_bytes_processed_total", "Total bytes processed", ["source_type","source_id","source_name"])
        self.SINK_QUEUE_SIZE = Gauge("log_capturer_sink_queue_size", "Queue size", ["sink_name"])
        self.SINK_DISK_BUFFER_BYTES = Gauge("log_capturer_sink_disk_buffer_size_bytes", "Disk buffer size", ["sink_name"])
        self.LOGS_DLQ = Counter("log_capturer_logs_dlq_total", "DLQ logs", ["source_type","reason"])
        self.PROCESSING_ERRORS = Counter("log_capturer_processing_errors_total", "Processing errors", ["source_type","source_id"])
        self.PROCESSING_TIME = Histogram("log_capturer_processing_time_seconds", "Processing time", ["source_type","source_id"])
        self.PROCESSING_BATCH_SIZE = Histogram("log_capturer_processing_batch_size", "Batch size", ["sink_name"])
        self.PROCESSING_DROPPED = Counter("log_capturer_processing_dropped_total", "Dropped logs")
        # NOVO: latência de envio aos sinks
        self.SINK_SEND_LATENCY = Histogram("log_capturer_sink_send_latency_seconds", "Send latency", ["sink_name"])
        self.CONTAINER_INFO = Gauge("log_capturer_container_info","Container info",["task_name","container_name","image"])
        self.FILE_INFO = Gauge("log_capturer_file_info","File info",["task_name","filepath"])
        self.TASK_HEALTH = Gauge("log_capturer_task_health","Task health",["task_name"])
        self.TASK_RESTARTS = Counter("log_capturer_task_restarts_total","Task restarts",["task_name","container_name"])
        self.CIRCUIT_BREAKER_STATE = Gauge("log_capturer_circuit_breaker_state","CB state",["breaker_name"])
        self.CIRCUIT_BREAKER_FAILURES = Counter("log_capturer_circuit_breaker_failures_total","CB failures",["breaker_name"])
        self.HTTP_SESSION_POOL_ACTIVE = Gauge("log_capturer_http_session_pool_active","HTTP sessions active")
        self.HTTP_SESSION_POOL_AVAILABLE = Gauge("log_capturer_http_session_pool_available","HTTP sessions available")
        self.FILE_IO_PENDING_WRITES = Gauge("log_capturer_file_io_pending_writes","Pending writes",["file_path"])
        self.THROTTLING_DELAY = Histogram("log_capturer_throttling_delay_seconds","Throttle delay",["sink_name"])
        self.CLEANUP_FILES_REMOVED = Counter("log_capturer_cleanup_files_removed_total","Cleanup removed files")
        self.CLEANUP_BYTES_FREED = Counter("log_capturer_cleanup_bytes_freed_total","Cleanup bytes freed")
        self.FILE_POSITION_BYTES = Gauge("log_capturer_file_position_bytes","File offset",["filepath"])
        self.POSITION_FILE_UPDATES = Counter("log_capturer_position_file_updates_total","Position updates",["filepath"])
        self.POSITION_FILE_ERRORS = Counter("log_capturer_position_file_errors_total","Position errors",["filepath","operation"])
        self.CONFIG_VALIDATION_ERRORS = Counter("log_capturer_config_validation_errors_total","Config validation errors")
        self.RETRY_ATTEMPTS = Counter("log_capturer_retry_attempts_total","Retry attempts",["operation","attempt"])
        self.CORRELATION_IDS_GENERATED = Counter("log_capturer_correlation_ids_generated_total","Correlation IDs")
        self.STRUCTURED_LOGS_EMITTED = Counter("log_capturer_structured_logs_emitted_total","Structured logs",["level"])
        self.SYSTEM_CPU = Gauge("log_capturer_system_cpu_percent","CPU usage")
        self.SYSTEM_MEM = Gauge("log_capturer_system_mem_bytes","Memory usage")
        self.SYSTEM_DISK = Gauge("log_capturer_system_disk_usage","Disk usage",["mountpoint"])
        # NOVO: métricas do container (cgroup)
        self.CONTAINER_MEM_BYTES = Gauge("log_capturer_container_mem_bytes", "Container memory usage (cgroup) in bytes")
        self.CONTAINER_MEM_LIMIT_BYTES = Gauge("log_capturer_container_mem_limit_bytes", "Container memory limit (cgroup) in bytes")
        self.CONTAINER_MEM_PERCENT = Gauge("log_capturer_container_mem_percent", "Container memory percent (cgroup)")
        # NOVO: CPU do container (cgroup)
        self.CONTAINER_CPU_PERCENT = Gauge("log_capturer_container_cpu_percent", "Container CPU usage percent (cgroup)")
        self.INFO.labels(version="10.0-complete-enterprise").set(1)
        # NOVO: logger e flags internas
        self.logger = StructuredLogger("metrics")
        self._cgroup_mode_logged = False
        # NOVO: estado para cálculo de CPU por delta
        self._cpu_prev_time = None
        self._cpu_prev_usage_ns = None
        self._cpu_limit_cpus = None

    # NOVO: leitura de cgroup v2 de memória
    def _read_cgroup_v2_memory(self):
        base = Path("/sys/fs/cgroup")
        usage_p = base / "memory.current"
        limit_p = base / "memory.max"
        if not usage_p.exists() or not limit_p.exists():
            return None
        try:
            used = int(usage_p.read_text().strip() or "0")
            limit_txt = limit_p.read_text().strip()
            limit = None if limit_txt == "max" else int(limit_txt or "0")
            if not self._cgroup_mode_logged:
                self.logger.info("cgroup v2 detectado para memória", usage_path=str(usage_p), limit_path=str(limit_p))
                self._cgroup_mode_logged = True
            return used, limit
        except Exception as e:
            self.logger.error("Falha ao ler cgroup v2 de memória", error=str(e))
            return None

    # NOVO: leitura de cgroup v1 (paths padrão)
    def _read_cgroup_v1_memory(self):
        base = Path("/sys/fs/cgroup/memory")
        usage_p = base / "memory.usage_in_bytes"
        limit_p = base / "memory.limit_in_bytes"
        if not usage_p.exists() or not limit_p.exists():
            return None
        try:
            used = int(usage_p.read_text().strip() or "0")
            limit = int(limit_p.read_text().strip() or "0")
            if not self._cgroup_mode_logged:
                self.logger.info("cgroup v1 detectado para memória", usage_path=str(usage_p), limit_path=str(limit_p))
                self._cgroup_mode_logged = True
            return used, limit
        except Exception as e:
            self.logger.error("Falha ao ler cgroup v1 de memória", error=str(e))
            return None

    # NOVO: helper unificado
    def _read_cgroup_memory(self):
        # Tenta v2, depois v1
        res = self._read_cgroup_v2_memory()
        if res is not None:
            return res
        res = self._read_cgroup_v1_memory()
        if res is not None:
            return res
        # Fallback: tenta deduzir via /proc/self/cgroup para v1 (melhor esforço)
        try:
            mem_path = None
            for line in Path("/proc/self/cgroup").read_text().splitlines():
                # Formato v1: 5:memory:/docker/<id>
                parts = line.split(":")
                if len(parts) == 3 and "memory" in parts[1]:
                    mem_path = parts[2].lstrip("/")
                    break
            if mem_path:
                base = Path("/sys/fs/cgroup/memory") / mem_path
                usage_p = base / "memory.usage_in_bytes"
                limit_p = base / "memory.limit_in_bytes"
                if usage_p.exists() and limit_p.exists():
                    used = int(usage_p.read_text().strip() or "0")
                    limit = int(limit_p.read_text().strip() or "0")
                    if not self._cgroup_mode_logged:
                        self.logger.info("cgroup v1 detectado (via /proc/self/cgroup)", usage_path=str(usage_p), limit_path=str(limit_p))
                        self._cgroup_mode_logged = True
                    return used, limit
        except Exception:
            pass
        # Não foi possível detectar cgroup
        if not self._cgroup_mode_logged:
            self.logger.info("Métricas de memória do container indisponíveis (cgroup não detectado); mantendo apenas métricas de sistema")
            self._cgroup_mode_logged = True
        return None

    # NOVO: leitura de cgroup v2 de CPU
    def _read_cgroup_v2_cpu(self):
        """
        Retorna (usage_ns, cpus_limit_float | None)
        cpu.stat: usage_usec
        cpu.max:  'max' ou '<quota> <period>'
        """
        base = Path("/sys/fs/cgroup")
        stat_p = base / "cpu.stat"
        max_p = base / "cpu.max"
        if not stat_p.exists():
            return None
        try:
            usage_ns = None
            for line in stat_p.read_text().splitlines():
                k, _, v = line.partition(" ")
                if k.strip() in ("usage_usec", "usage_usec:"):
                    usage_ns = int(v.strip()) * 1000  # usec -> nsec
                    break
            if usage_ns is None:
                # algumas distros usam 'usage' (nsec)
                for line in stat_p.read_text().splitlines():
                    k, _, v = line.partition(" ")
                    if k.strip().startswith("usage"):
                        usage_ns = int(v.strip())
                        break
            cpus_limit = None
            if max_p.exists():
                content = max_p.read_text().strip()
                parts = content.split()
                if len(parts) == 2 and parts[0] != "max":
                    quota_us = int(parts[0]); period_us = int(parts[1] or "100000")
                    if period_us > 0:
                        cpus_limit = max(0.1, quota_us / period_us)
            return (usage_ns if usage_ns is not None else 0, cpus_limit)
        except Exception as e:
            self.logger.error("Falha ao ler CPU cgroup v2", error=str(e))
            return None

    # NOVO: CPU cgroup v1
    def _read_cgroup_v1_cpu(self):
        """
        Retorna (usage_ns, cpus_limit_float | None)
        cpuacct.usage: nsec
        cpu.cfs_quota_us / cpu.cfs_period_us para limite.
        """
        acct_base = Path("/sys/fs/cgroup/cpuacct")
        cpu_base = Path("/sys/fs/cgroup/cpu")
        usage_p = acct_base / "cpuacct.usage"
        quota_p = cpu_base / "cpu.cfs_quota_us"
        period_p = cpu_base / "cpu.cfs_period_us"
        if not usage_p.exists():
            return None
        try:
            usage_ns = int(usage_p.read_text().strip() or "0")
            cpus_limit = None
            if quota_p.exists() and period_p.exists():
                quota_us = int(quota_p.read_text().strip() or "-1")
                period_us = int(period_p.read_text().strip() or "100000")
                if quota_us > 0 and period_us > 0:
                    cpus_limit = max(0.1, quota_us / period_us)
            return (usage_ns, cpus_limit)
        except Exception as e:
            self.logger.error("Falha ao ler CPU cgroup v1", error=str(e))
            return None

    # NOVO: helper CPU unificado
    def _read_cgroup_cpu(self):
        res = self._read_cgroup_v2_cpu()
        if res is not None:
            if not self._cgroup_mode_logged:
                self.logger.info("cgroup v2 detectado para CPU")
                self._cgroup_mode_logged = True
            return res
        res = self._read_cgroup_v1_cpu()
        if res is not None:
            if not self._cgroup_mode_logged:
                self.logger.info("cgroup v1 detectado para CPU")
                self._cgroup_mode_logged = True
            return res
        if not self._cgroup_mode_logged:
            self.logger.info("Métricas de CPU do container indisponíveis (cgroup não detectado); mantendo apenas métricas de sistema")
            self._cgroup_mode_logged = True
        return None

    # NOVO: cálculo de deltas para CPU
    def _calculate_cpu_delta(self, cpu_now, usage_ns):
        if self._cpu_prev_time is not None and self._cpu_prev_usage_ns is not None and usage_ns >= self._cpu_prev_usage_ns:
            dt = max(1e-3, cpu_now - self._cpu_prev_time)  # evita div/0
            # CPUs disponíveis (limite do cgroup se houver, senão cpu_count lógico)
            if self._cpu_limit_cpus is None:
                self._cpu_limit_cpus = float(os.cpu_count() or 1)
            denom_ns = dt * self._cpu_limit_cpus * 1e9
            percent = (usage_ns - self._cpu_prev_usage_ns) / denom_ns * 100.0
            percent = max(0.0, min(percent, 100.0 * self._cpu_limit_cpus))
            return percent
        return None

    async def start_system_metrics(self, interval=10):
        """Coleta periódica de métricas do sistema (método da classe)"""
        while True:
            try:
                # CPU (sistema)
                self.SYSTEM_CPU.set(psutil.cpu_percent(interval=None))
                # Memória (sistema)
                mem = psutil.virtual_memory()
                self.SYSTEM_MEM.set(mem.used)
                # Disco (sistema) - ignora fstypes não relevantes
                try:
                    ignore_fstypes = {"proc", "sysfs", "devtmpfs", "devpts", "cgroup", "cgroup2", "overlay", "squashfs", "tmpfs", "aufs", "rpc_pipefs", "nsfs", "tracefs", "fusectl"}
                    seen = set()
                    for part in psutil.disk_partitions(all=False):
                        try:
                            if part.fstype.lower() in ignore_fstypes:
                                continue
                            mp = part.mountpoint
                            if not mp or mp in seen:
                                continue
                            seen.add(mp)
                            usage = psutil.disk_usage(mp)
                            self.SYSTEM_DISK.labels(mountpoint=mp).set(usage.percent)
                        except Exception:
                            continue
                except Exception as e:
                    print(f"Erro ao varrer discos: {str(e)}")

                # NOVO: memória do container (cgroup)
                try:
                    cg = self._read_cgroup_memory()
                    if cg is not None:
                        used, limit = cg
                        self.CONTAINER_MEM_BYTES.set(float(used))
                        if limit and limit > 0:
                            self.CONTAINER_MEM_LIMIT_BYTES.set(float(limit))
                            pct = (used / limit) * 100.0
                            self.CONTAINER_MEM_PERCENT.set(pct)
                            self.logger.debug("Métrica de memória do container atualizada", used_bytes=used, limit_bytes=limit, percent=round(pct,2))
                        else:
                            self.CONTAINER_MEM_LIMIT_BYTES.set(0.0)
                            self.CONTAINER_MEM_PERCENT.set(0.0)
                except Exception as e:
                    self.logger.error("Falha ao atualizar métricas de memória do container", error=str(e))

                # NOVO: CPU do container (cgroup) com cálculo por delta
                try:
                    cpu_now = time.time()
                    cpu_data = self._read_cgroup_cpu()
                    if cpu_data is not None:
                        usage_ns, cpus_limit = cpu_data
                        percent = self._calculate_cpu_delta(cpu_now, usage_ns)
                        if percent is not None:
                            # FIX: não fazer round em None
                            safe_cpus = round(cpus_limit, 2) if isinstance(cpus_limit, (int, float)) else cpus_limit
                            self.CONTAINER_CPU_PERCENT.set(percent)
                            self.logger.debug("Métrica de CPU do container atualizada", percent=round(percent,2), cpus_limit=safe_cpus)
                        self._cpu_prev_time = cpu_now
                        self._cpu_prev_usage_ns = usage_ns
                        if self._cpu_limit_cpus is None and cpus_limit is not None:
                            self._cpu_limit_cpus = cpus_limit
                except Exception as e:
                    self.logger.error("Falha ao atualizar métricas de CPU do container", error=str(e))

            except Exception as e:
                print(f"Erro na coleta de métricas do sistema: {str(e)}")
            await asyncio.sleep(interval)

metrics = Metrics()