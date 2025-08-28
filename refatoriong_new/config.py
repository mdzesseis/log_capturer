"""
config.py - Configuração central do agente de monitoramento.
- Carrega variáveis de ambiente e define diretórios, portas, sinks, processamento, robustez.
- Facilita modo debug para testes rápidos.
"""

import os
import logging
from pathlib import Path

# Facilitar modo debug para testes
DEBUG_MODE = os.getenv("DEBUG_MODE", "false").lower() == "true"

logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s'
)

# Base dirs e portas
BASE_DIR = Path(os.getenv("LOG_CAPTURER_DIR", "/logs"))
API_PORT = int(os.getenv("API_PORT", 8080))
METRICS_PORT = int(os.getenv("METRICS_PORT", 8001))

# Docker
DOCKER_LABEL_FILTER_ENABLED = os.getenv("DOCKER_LABEL_FILTER_ENABLED", "false").lower() == "true"
DOCKER_LABEL_FILTER = os.getenv("DOCKER_LABEL_FILTER", "")
DOCKER_CONNECTION_REQUIRED = os.getenv("DOCKER_CONNECTION_REQUIRED", "false").lower() == "true"
DOCKER_CONNECTION_TIMEOUT = int(os.getenv("DOCKER_CONNECTION_TIMEOUT", "10"))
# NOVO: limites e pool de conexões Docker
DOCKER_API_RATE_LIMIT = int(os.getenv("DOCKER_API_RATE_LIMIT", "10"))
DOCKER_API_BURST_SIZE = int(os.getenv("DOCKER_API_BURST_SIZE", "20"))
DOCKER_CONNECTION_POOL_SIZE = int(os.getenv("DOCKER_CONNECTION_POOL_SIZE", "5"))
DOCKER_CONNECTION_POOL_MAX_SIZE = int(os.getenv("DOCKER_CONNECTION_POOL_MAX_SIZE", str(max(10, DOCKER_CONNECTION_POOL_SIZE * 5))))
DOCKER_METADATA_CACHE_TTL = int(os.getenv("DOCKER_METADATA_CACHE_TTL", "300"))

# Files
FILES_CONFIG_PATH = os.getenv("FILES_CONFIG_PATH", "/etc/log_capturer/files.yaml")
POSITIONS_DIR = BASE_DIR / "positions"
POSITION_SAVE_INTERVAL = float(os.getenv("BUFFER_SYNC_INTERVAL", "5.0"))

# Processing
PROCESSING_ENABLED = os.getenv("PROCESSING_ENABLED", "false").lower() == "true"
PROCESSING_WORKERS = int(os.getenv("PROCESSING_WORKERS", "0"))
PIPELINE_CONFIG_FILE = Path(os.getenv("PIPELINE_CONFIG_FILE", "/etc/log_capturer/pipelines.yaml"))
DLQ_ENABLED = os.getenv("DLQ_ENABLED", "true").lower() == "true"

# Secrets
SECRETS_MANAGER = os.getenv("SECRETS_MANAGER", "env")
VAULT_ADDR = os.getenv("VAULT_ADDR")
VAULT_TOKEN = os.getenv("VAULT_TOKEN")
AWS_REGION = os.getenv("AWS_REGION")

# Task/Circuit Breaker
CIRCUIT_BREAKER_ENABLED = os.getenv("CIRCUIT_BREAKER_ENABLED", "true").lower() == "true"
CIRCUIT_BREAKER_FAILURE_THRESHOLD = int(os.getenv("CIRCUIT_BREAKER_FAILURE_THRESHOLD", "5"))
CIRCUIT_BREAKER_RECOVERY_TIMEOUT = int(os.getenv("CIRCUIT_BREAKER_RECOVERY_TIMEOUT", "60"))
# NOVO: parâmetros extras para restart/rate limiting
TASK_MAX_RESTARTS_PER_HOUR = int(os.getenv("TASK_MAX_RESTARTS_PER_HOUR", "10"))
TASK_CIRCUIT_BREAKER_DURATION = int(os.getenv("TASK_CIRCUIT_BREAKER_DURATION", "1800"))

TASK_RESTART_ENABLED = os.getenv("TASK_RESTART_ENABLED", "true").lower() == "true"
TASK_MAX_RESTART_COUNT = int(os.getenv("TASK_MAX_RESTART_COUNT", "7"))  # Aumentado para 7
TASK_RESTART_BACKOFF_BASE = float(os.getenv("TASK_RESTART_BACKOFF_BASE", "2.0"))
TASK_HEALTH_CHECK_INTERVAL = int(os.getenv("TASK_HEALTH_CHECK_INTERVAL", "30"))

# Otimizações
HTTP_SESSION_POOL_SIZE = int(os.getenv("HTTP_SESSION_POOL_SIZE", "30"))
HTTP_CONNECTION_TIMEOUT = int(os.getenv("HTTP_CONNECTION_TIMEOUT", "60"))
HTTP_REQUEST_TIMEOUT = int(os.getenv("HTTP_REQUEST_TIMEOUT", "20"))
HTTP_CONNECTOR_LIMIT = int(os.getenv("HTTP_CONNECTOR_LIMIT", "100"))
HTTP_CONNECTOR_LIMIT_PER_HOST = int(os.getenv("HTTP_CONNECTOR_LIMIT_PER_HOST", "20"))
FILE_IO_THREAD_POOL_SIZE = int(os.getenv("FILE_IO_THREAD_POOL_SIZE", "20"))
BACKPRESSURE_THRESHOLD = float(os.getenv("BACKPRESSURE_THRESHOLD", "0.8"))
CLEANUP_INTERVAL_SECONDS = int(os.getenv("CLEANUP_INTERVAL", "300"))
CLEANUP_AGE_THRESHOLD_SECONDS = int(os.getenv("CLEANUP_AGE_THRESHOLD", "3600"))
ADAPTIVE_THROTTLING_ENABLED = os.getenv("ADAPTIVE_THROTTLING_ENABLED", "true").lower() == "true"
THROTTLE_SLEEP_BASE = float(os.getenv("THROTTLE_SLEEP_BASE", "0.1"))
THROTTLE_SLEEP_MAX = float(os.getenv("THROTTLE_SLEEP_MAX", "5.0"))

# Robustez
STRUCTURED_LOGGING_ENABLED = os.getenv("STRUCTURED_LOGGING_ENABLED", "true").lower() == "true"
CORRELATION_ID_ENABLED = os.getenv("CORRELATION_ID_ENABLED", "true").lower() == "true"
HEALTH_METRICS_ENABLED = os.getenv("HEALTH_METRICS_ENABLED", "true").lower() == "true"
CONFIG_VALIDATION_STRICT = os.getenv("CONFIG_VALIDATION_STRICT", "false").lower() == "true"
RETRY_JITTER_ENABLED = os.getenv("RETRY_JITTER_ENABLED", "true").lower() == "true"
RETRY_JITTER_MAX_MS = int(os.getenv("RETRY_JITTER_MAX_MS", "1000"))

# NOVO: Hot-path logging (debug amostrado)
HOTPATH_DEBUG_ENABLED = os.getenv("HOTPATH_DEBUG_ENABLED", "true").lower() == "true"
HOTPATH_DEBUG_SAMPLE_N = int(os.getenv("HOTPATH_DEBUG_SAMPLE_N", "100"))

# NOVO: limites para ajuste de timestamp (evita "entry too far behind" no Loki)
TIMESTAMP_MAX_PAST_AGE_SECONDS = int(os.getenv("TIMESTAMP_MAX_PAST_AGE_SECONDS", "7200"))      # 2h
TIMESTAMP_MAX_FUTURE_AGE_SECONDS = int(os.getenv("TIMESTAMP_MAX_FUTURE_AGE_SECONDS", "60"))    # 1min
# NOVO: controle do ajuste de timestamp via env
TIMESTAMP_CLAMP_ENABLED = os.getenv("TIMESTAMP_CLAMP_ENABLED", "true").lower() == "true"
# NOVO: política de rejeição por timestamp no Loki (DLQ ou drop)
TIMESTAMP_CLAMP_DLQ = os.getenv("TIMESTAMP_CLAMP_DLQ", "false").lower() == "true"

# NOVO: otimizações de pipeline (ordenação/early-exit)
PIPELINE_OPTIMIZE_ORDER_ENABLED = os.getenv("PIPELINE_OPTIMIZE_ORDER_ENABLED", "true").lower() == "true"
PIPELINE_EARLY_EXIT_ENABLED = os.getenv("PIPELINE_EARLY_EXIT_ENABLED", "true").lower() == "true"

# Sinks
LOKI_SINK_ENABLED = os.getenv("LOKI_SINK_ENABLED", "false").lower() == "true"
LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100/loki/api/v1/push")
LOKI_SINK_QUEUE_SIZE = int(os.getenv("LOKI_SINK_QUEUE_SIZE", 5000))
LOKI_BATCH_SIZE = int(os.getenv("LOKI_BATCH_SIZE", 1000))
LOKI_BATCH_TIMEOUT = float(os.getenv("LOKI_BATCH_TIMEOUT", 1.5))
LOKI_BUFFER_DIR = Path(os.getenv("LOKI_BUFFER_DIR", str(BASE_DIR / "loki_buffer")))
LOKI_BUFFER_MAX_SIZE_MB = int(os.getenv("LOKI_BUFFER_MAX_SIZE_MB", "128"))
# NOVO: limites duros do batch adaptativo (com default seguro)
LOKI_BATCH_HARD_MAX = int(os.getenv("LOKI_BATCH_HARD_MAX", str(LOKI_BATCH_SIZE)))
LOKI_BATCH_HARD_MIN = int(os.getenv("LOKI_BATCH_HARD_MIN", str(max(1, LOKI_BATCH_SIZE // 4))))

# NOVO: Rate limiting adaptativo nos sinks
SINK_RATE_LIMIT_ENABLED = os.getenv("SINK_RATE_LIMIT_ENABLED", "true").lower() == "true"
SINK_RATE_LIMIT_RPS = float(os.getenv("SINK_RATE_LIMIT_RPS", "10"))              # tokens/s
SINK_RATE_LIMIT_BURST = float(os.getenv("SINK_RATE_LIMIT_BURST", "20"))          # burst de tokens
SINK_RATE_LIMIT_MIN_RPS = float(os.getenv("SINK_RATE_LIMIT_MIN_RPS", "1"))
SINK_RATE_LIMIT_MAX_RPS = float(os.getenv("SINK_RATE_LIMIT_MAX_RPS", "200"))
SINK_RATE_LIMIT_LATENCY_TARGET_MS = float(os.getenv("SINK_RATE_LIMIT_LATENCY_TARGET_MS", "500"))
SINK_RATE_LIMIT_BYTES_PER_TOKEN = int(os.getenv("SINK_RATE_LIMIT_BYTES_PER_TOKEN", str(64 * 1024)))  # 64KiB/token

# NOVO: compressão HTTP
SINK_HTTP_COMPRESSION_ENABLED = os.getenv("SINK_HTTP_COMPRESSION_ENABLED", "true").lower() == "true"
SINK_HTTP_COMPRESSION_MIN_BYTES = int(os.getenv("SINK_HTTP_COMPRESSION_MIN_BYTES", str(16 * 1024)))   # 16KiB
SINK_HTTP_COMPRESSION_LEVEL = int(os.getenv("SINK_HTTP_COMPRESSION_LEVEL", "6"))
# NOVO: algoritmo de compressão para HTTP (ex.: gzip|zstd). Para Loki, gzip é o mais compatível.
SINK_HTTP_COMPRESSION_ALGO = os.getenv("SINK_HTTP_COMPRESSION_ALGO", "gzip").lower()
# NOVO: compressão adaptativa (nível) por tamanho/CPU
SINK_HTTP_COMPRESSION_ADAPTIVE = os.getenv("SINK_HTTP_COMPRESSION_ADAPTIVE", "true").lower() == "true"

# NOVO: compressão do buffer em disco (zstd|lz4|gzip)
DISK_BUFFER_COMPRESSION_ALGO = os.getenv("DISK_BUFFER_COMPRESSION_ALGO", "zstd").lower()
DISK_BUFFER_COMPRESSION_LEVEL = int(os.getenv("DISK_BUFFER_COMPRESSION_LEVEL", "3"))

ELASTICSEARCH_SINK_ENABLED = os.getenv("ELASTICSEARCH_SINK_ENABLED", "false").lower() == "true"
ELASTICSEARCH_HOSTS = os.getenv("ELASTICSEARCH_HOSTS", "http://elasticsearch:9200").split(',')
ELASTICSEARCH_INDEX_PREFIX = os.getenv("ELASTICSEARCH_INDEX_PREFIX", "log-capturer")
ELASTICSEARCH_SINK_QUEUE_SIZE = int(os.getenv("ELASTICSEARCH_SINK_QUEUE_SIZE", 1000))
ELASTICSEARCH_BUFFER_DIR = Path(os.getenv("ELASTICSEARCH_BUFFER_DIR", str(BASE_DIR / "es_buffer")))
ELASTICSEARCH_BUFFER_MAX_SIZE_MB = int(os.getenv("ELASTICSEARCH_BUFFER_MAX_SIZE_MB", "1024"))

SPLUNK_SINK_ENABLED = os.getenv("SPLUNK_SINK_ENABLED", "false").lower() == "true"
SPLUNK_HEC_URL = os.getenv("SPLUNK_HEC_URL", "http://splunk:8088/services/collector")
SPLUNK_HEC_TOKEN_SECRET = os.getenv("SPLUNK_HEC_TOKEN_SECRET", "splunk_hec_token")
SPLUNK_SINK_QUEUE_SIZE = int(os.getenv("SPLUNK_SINK_QUEUE_SIZE", 1000))
SPLUNK_BUFFER_DIR = Path(os.getenv("SPLUNK_BUFFER_DIR", str(BASE_DIR / "splunk_buffer")))
SPLUNK_BUFFER_MAX_SIZE_MB = int(os.getenv("SPLUNK_BUFFER_MAX_SIZE_MB", "1024"))

LOCAL_SINK_ENABLED = os.getenv("LOCAL_SINK_ENABLED", "true").lower() == "true"
LOCAL_SINK_QUEUE_SIZE = int(os.getenv("LOCAL_SINK_QUEUE_SIZE", 1000))
LOCAL_SINK_BATCH_SIZE = int(os.getenv("BUFFER_WRITE_BATCH_SIZE", "100"))
LOCAL_SINK_RETENTION_DAYS = int(os.getenv("LOCAL_SINK_RETENTION_DAYS", "7"))
# NOVO: limite do buffer local e limpeza proativa
LOCAL_SINK_MAX_SIZE_MB = int(os.getenv("LOCAL_SINK_MAX_SIZE_MB", "256"))
PROACTIVE_CLEANUP_ENABLED = os.getenv("PROACTIVE_CLEANUP_ENABLED", "true").lower() == "true"
# NOVO: rotação por tamanho (MB) para arquivos finais da LocalFileSink
LOCAL_SINK_MAX_FILE_SIZE_MB = int(os.getenv("LOCAL_SINK_MAX_FILE_SIZE_MB", "32"))

# Controle global do monitoramento de arquivos
FILE_MONITOR_ENABLED = os.getenv("FILE_MONITOR_ENABLED", "true").lower() == "true"

# NOVO: identificação do próprio container e guarda contra realimentação
SELF_ID_SHORT = os.getenv("HOSTNAME", "")[:12]
SELF_CONTAINER_NAME = os.getenv("SELF_CONTAINER_NAME", "log_capturer")
SELF_FEEDBACK_GUARD = os.getenv("SELF_FEEDBACK_GUARD", "true").lower() == "true"

# NOVO: Configurações para limpeza de tasks com verificação de segurança
ORPHANED_TASK_CLEANUP_INTERVAL = int(os.getenv("ORPHANED_TASK_CLEANUP_INTERVAL", "300"))  # 5 minutos

# NOVO: Configuração inteligente do Docker Socket
def _get_docker_socket_url():
    """Detecta socket Docker sem causar import circular"""
    # 1. DOCKER_HOST (padrão Docker)
    docker_host = os.getenv("DOCKER_HOST")
    if docker_host:
        return docker_host.replace("tcp://", "http://") if docker_host.startswith("tcp://") else docker_host
    # 2. DOCKER_SOCKET_URL (nossa configuração)
    socket_url = os.getenv("DOCKER_SOCKET_URL")
    if socket_url:
        return socket_url
    # 3. Socket Unix padrão
    unix_socket = "/var/run/docker.sock"
    if Path(unix_socket).exists():
        return f"unix://{unix_socket}" 
    # 4. Fallback
    return "http://localhost:2375"

DOCKER_SOCKET_URL = _get_docker_socket_url()

# Permite override manual se necessário
DOCKER_SOCKET_URL_OVERRIDE = os.getenv("DOCKER_SOCKET_URL_OVERRIDE")
if DOCKER_SOCKET_URL_OVERRIDE:
    DOCKER_SOCKET_URL = DOCKER_SOCKET_URL_OVERRIDE

# Dirs necessários
for dir_path in [BASE_DIR, LOKI_BUFFER_DIR, ELASTICSEARCH_BUFFER_DIR, SPLUNK_BUFFER_DIR, BASE_DIR / "dlq", POSITIONS_DIR]:
    try:
        dir_path.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    except Exception:
        pass

# NOVO: política de retry/backoff para batches persistidos (recovery)
SINK_PERSISTENCE_ENABLED = os.getenv("SINK_PERSISTENCE_ENABLED", "true").lower() == "true"
SINK_PERSISTENCE_RECOVERY_MAX_RETRIES = int(os.getenv("SINK_PERSISTENCE_RECOVERY_MAX_RETRIES", "10"))
SINK_PERSISTENCE_RECOVERY_BACKOFF_BASE = float(os.getenv("SINK_PERSISTENCE_RECOVERY_BACKOFF_BASE", "1.0"))  # segundos
SINK_PERSISTENCE_RECOVERY_BACKOFF_MAX = float(os.getenv("SINK_PERSISTENCE_RECOVERY_BACKOFF_MAX", "30.0"))  # segundos
