#!/bin/bash

# Script para monitorar o tamanho dos dados do Loki
LOKI_DATA_DIR="/var/lib/docker/volumes/ssw-monitoring-stack-new-project_loki-data/_data"
MAX_SIZE_GB=5
CLEANUP_THRESHOLD_PERCENT=80

# Função para converter bytes para GB
bytes_to_gb() {
    echo "scale=2; $1 / 1024 / 1024 / 1024" | bc -l
}

# Verificar se o diretório existe
if [ ! -d "$LOKI_DATA_DIR" ]; then
    echo "ERROR: Loki data directory not found: $LOKI_DATA_DIR"
    exit 1
fi

# Obter tamanho atual
CURRENT_SIZE_BYTES=$(du -sb "$LOKI_DATA_DIR" 2>/dev/null | cut -f1)
if [ -z "$CURRENT_SIZE_BYTES" ]; then
    echo "ERROR: Could not determine directory size"
    exit 1
fi

CURRENT_SIZE_GB=$(bytes_to_gb $CURRENT_SIZE_BYTES)

# Calcular limite
MAX_SIZE_BYTES=$((MAX_SIZE_GB * 1024 * 1024 * 1024))
THRESHOLD_BYTES=$((MAX_SIZE_BYTES * CLEANUP_THRESHOLD_PERCENT / 100))

echo "Loki Data Size Monitor - $(date)"
echo "======================"
echo "Current size: ${CURRENT_SIZE_GB}GB"
echo "Max size: ${MAX_SIZE_GB}GB"
echo "Threshold: $(bytes_to_gb $THRESHOLD_BYTES)GB (${CLEANUP_THRESHOLD_PERCENT}%)"
echo "Usage: $(echo "scale=1; $CURRENT_SIZE_BYTES * 100 / $MAX_SIZE_BYTES" | bc -l)%"

# Verificar se precisa de limpeza
if [ "$CURRENT_SIZE_BYTES" -gt "$THRESHOLD_BYTES" ]; then
    echo "WARNING: Loki data size exceeded threshold!"
    echo "Triggering cleanup via API..."
    
    # Verificar se Loki está rodando
    if ! curl -s -f "http://localhost:3100/ready" > /dev/null; then
        echo "ERROR: Loki is not responding at http://localhost:3100"
        exit 1
    fi
    
    # Chamar API do Loki para limpeza (logs mais antigos que 3 dias)
    CLEANUP_RESPONSE=$(curl -s -X POST "http://localhost:3100/loki/api/v1/delete" \
         -H "Content-Type: application/json" \
         -d '{
           "query": "{job=\"container_monitoring\"}",
           "start": "'$(date -d '7 days ago' --iso-8601)'T00:00:00Z",
           "end": "'$(date -d '3 days ago' --iso-8601)'T23:59:59Z"
         }')
    
    echo "Cleanup API response: $CLEANUP_RESPONSE"
    
    # Forçar compactação
    echo "Triggering compaction..."
    COMPACTION_RESPONSE=$(curl -s -X POST "http://localhost:3100/compactor/ring" || echo "Compaction trigger failed")
    echo "Compaction response: $COMPACTION_RESPONSE"
    
    echo "Cleanup operations completed."
else
    echo "Storage usage is within acceptable limits."
fi

# Criar diretório para métricas se não existir
mkdir -p /tmp/monitoring

# Salvar métricas para coleta pelo Prometheus
cat > /tmp/monitoring/loki_metrics.prom << EOF
# HELP loki_data_size_bytes Total size of Loki data in bytes
# TYPE loki_data_size_bytes gauge
loki_data_size_bytes $CURRENT_SIZE_BYTES

# HELP loki_data_size_gb Total size of Loki data in GB
# TYPE loki_data_size_gb gauge
loki_data_size_gb $CURRENT_SIZE_GB

# HELP loki_max_size_bytes Maximum allowed size of Loki data in bytes
# TYPE loki_max_size_bytes gauge
loki_max_size_bytes $MAX_SIZE_BYTES

# HELP loki_usage_percent Current usage percentage of Loki data
# TYPE loki_usage_percent gauge
loki_usage_percent $(echo "scale=2; $CURRENT_SIZE_BYTES * 100 / $MAX_SIZE_BYTES" | bc -l)
EOF

echo "Metrics saved to /tmp/monitoring/loki_metrics.prom"
