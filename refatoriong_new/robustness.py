import json
import time
import uuid
import random
import asyncio
import threading
import traceback
from datetime import datetime
from typing import Optional
from .config import (
    RETRY_JITTER_MAX_MS, CONFIG_VALIDATION_STRICT, BASE_DIR, 
    LOKI_SINK_ENABLED, LOKI_URL, DOCKER_LABEL_FILTER_ENABLED, 
    DOCKER_LABEL_FILTER, PROCESSING_ENABLED, PIPELINE_CONFIG_FILE,
    # NOVO: importar flags de verificação
    STRUCTURED_LOGGING_ENABLED, CORRELATION_ID_ENABLED, 
    HEALTH_METRICS_ENABLED, RETRY_JITTER_ENABLED, DEBUG_MODE
)

# REMOVIDO: from .metrics import metrics

# NOVO: helper para importar métricas de forma preguiçosa e evitar import circular
def _get_metrics():
    try:
        from .metrics import metrics  # import local evita ciclo na carga do módulo
        return metrics
    except Exception:
        return None

class CorrelationContext:
    # MODIFICADO: Substituir threading.local por um dicionário com chaves baseadas em IDs de tarefas
    _context = {}  # Usar um dict global com chaves baseadas em IDs de task
    _cleanup_interval = 3600  # Segundos (1 hora)
    _last_cleanup = time.time()
    
    @staticmethod
    def generate_correlation_id() -> str:
        # NOVO: Verificar CORRELATION_ID_ENABLED
        if not CORRELATION_ID_ENABLED:
            return "disabled"
            
        cid = f"corr-{uuid.uuid4().hex[:16]}"
        m = _get_metrics()
        if m:
            try:
                m.CORRELATION_IDS_GENERATED.inc()
            except Exception:
                pass
        return cid
        
    @staticmethod
    def set_correlation_id(corr_id: str):
        # NOVO: Verificar CORRELATION_ID_ENABLED
        if not CORRELATION_ID_ENABLED:
            return
            
        # MODIFICADO: Usar o ID da task atual como chave
        try:
            task = asyncio.current_task()
            if task:
                task_id = id(task)
                CorrelationContext._context[task_id] = corr_id
                
                # Limpeza periódica para evitar vazamento de memória
                now = time.time()
                if now - CorrelationContext._last_cleanup > CorrelationContext._cleanup_interval:
                    CorrelationContext._cleanup_old_entries()
                    CorrelationContext._last_cleanup = now
        except RuntimeError:
            # Fallback para threads comuns onde current_task não está disponível
            if not hasattr(CorrelationContext, '_thread_local'):
                CorrelationContext._thread_local = threading.local()
            CorrelationContext._thread_local.correlation_id = corr_id
        
    @staticmethod
    def get_correlation_id() -> Optional[str]:
        # NOVO: Verificar CORRELATION_ID_ENABLED
        if not CORRELATION_ID_ENABLED:
            return None
            
        # MODIFICADO: Buscar pelo ID da task atual
        try:
            task = asyncio.current_task()
            if task:
                task_id = id(task)
                return CorrelationContext._context.get(task_id)
        except RuntimeError:
            # Fallback para threads comuns
            if hasattr(CorrelationContext, '_thread_local'):
                return getattr(CorrelationContext._thread_local, 'correlation_id', None)
        return None
    
    @staticmethod
    def _cleanup_old_entries():
        """Limpar entradas antigas do dicionário para evitar vazamento de memória"""
        try:
            # Limpar apenas se houver muitas entradas (mais de 1000)
            if len(CorrelationContext._context) > 1000:
                # Manter apenas as entradas de tarefas ativas
                all_tasks = asyncio.all_tasks()
                active_ids = {id(task) for task in all_tasks}
                for task_id in list(CorrelationContext._context.keys()):
                    if task_id not in active_ids:
                        CorrelationContext._context.pop(task_id, None)
        except Exception:
            # Ignorar erros na limpeza
            pass

class StructuredLogger:
    def __init__(self, name: str):
        import logging
        self.logger = logging.getLogger(name)
        
    def _log(self, level: int, message: str, context: dict):
        # NOVO: Verificar STRUCTURED_LOGGING_ENABLED
        if not STRUCTURED_LOGGING_ENABLED:
            # Fallback para logging normal quando estruturado desativado
            self.logger.log(level, f"{message} {context}")
            return
            
        # NOVO: Integração com DEBUG_MODE - ignora logs de debug quando DEBUG_MODE=false
        if level == 10 and not DEBUG_MODE:  # 10 = logging.DEBUG
            return
            
        try:
            if not self.logger.isEnabledFor(level):
                return
        except Exception:
            pass

        corr_id = CorrelationContext.get_correlation_id() or "N/A"
        entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": level,
            "logger": self.logger.name,
            "message": message,
            "correlation_id": corr_id,
            **context
        }
        self.logger.log(level, json.dumps(entry, ensure_ascii=False))
        # NOVO: incrementa métrica via acesso preguiçoso
        try:
            m = _get_metrics()
            if m:
                lvl = {10:"DEBUG",20:"INFO",30:"WARNING",40:"ERROR",50:"CRITICAL"}.get(level,"INFO")
                m.STRUCTURED_LOGS_EMITTED.labels(level=lvl).inc()
        except Exception:
            pass
            
    def debug(self, msg, **ctx): self._log(10, msg, ctx)
    def info(self, msg, **ctx): self._log(20, msg, ctx)
    def warning(self, msg, **ctx): self._log(30, msg, ctx)
    def error(self, msg, **ctx): self._log(40, msg, ctx)

class ExponentialBackoffRetry:
    def __init__(self, max_retries: int = 3):
        self.max_retries = max_retries
        
    async def execute(self, func, *args, **kwargs):
        for attempt in range(self.max_retries):
            try:
                return await func(*args, **kwargs)
            except Exception:
                if attempt == self.max_retries - 1:
                    raise
                base_delay = 2 ** attempt
                
                # NOVO: Verificar RETRY_JITTER_ENABLED
                jitter = 0.0
                if RETRY_JITTER_ENABLED:
                    jitter = random.uniform(0, min(base_delay * 0.5, RETRY_JITTER_MAX_MS / 1000.0))
                    
                try:
                    m = _get_metrics()
                    if m:
                        m.RETRY_ATTEMPTS.labels(operation=getattr(func, "__name__", "op"), attempt=str(attempt+1)).inc()
                except Exception:
                    pass
                await asyncio.sleep(base_delay + jitter)

class ConfigValidator:
    def __init__(self):
        self.errors = []
        self.warnings = []
        
    def validate_all(self) -> bool:
        self.errors.clear()
        self.warnings.clear()
        if not BASE_DIR.exists():
            self.errors.append(f"Base directory missing: {BASE_DIR}")
        if LOKI_SINK_ENABLED and not LOKI_URL:
            self.errors.append("Loki enabled but no URL configured")
        if DOCKER_LABEL_FILTER_ENABLED and not DOCKER_LABEL_FILTER.strip():
            self.errors.append("DOCKER_LABEL_FILTER_ENABLED is true, but DOCKER_LABEL_FILTER is empty.")
        if PROCESSING_ENABLED and not PIPELINE_CONFIG_FILE.exists():
            self.warnings.append(f"Pipeline config missing: {PIPELINE_CONFIG_FILE}")
        if self.errors:
            # NOVO: incrementa métrica sem importar no topo do módulo
            try:
                m = _get_metrics()
                if m:
                    m.CONFIG_VALIDATION_ERRORS.inc(len(self.errors))
            except Exception:
                pass
        return len(self.errors) == 0

class HealthMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.last_heartbeat = time.time()
        self.components = {}
        
    def register_component(self, name: str):
        # NOVO: Verificar HEALTH_METRICS_ENABLED
        if not HEALTH_METRICS_ENABLED:
            return
            
        self.components[name] = {"status": "healthy", "last_check": time.time()}
        
    def update_component(self, name: str, status: str):
        # NOVO: Verificar HEALTH_METRICS_ENABLED
        if not HEALTH_METRICS_ENABLED:
            return
            
        if name in self.components:
            self.components[name]["status"] = status
            self.components[name]["last_check"] = time.time()
            
    def heartbeat(self):
        # NOVO: Verificar HEALTH_METRICS_ENABLED
        if not HEALTH_METRICS_ENABLED:
            return
            
        self.last_heartbeat = time.time()
        
    def get_health_status(self):
        # NOVO: Verificar HEALTH_METRICS_ENABLED
        if not HEALTH_METRICS_ENABLED:
            return {"status": "healthy", "metrics_enabled": False}
            
        now = time.time()
        return {
            "uptime": now - self.start_time,
            "last_heartbeat": now - self.last_heartbeat,
            "components": self.components,
            "status": "healthy" if all(c["status"] == "healthy" for c in self.components.values()) else "degraded"
        }
