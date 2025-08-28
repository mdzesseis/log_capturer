#!/bin/bash

# Loki Storage Monitor - Container Version
set -euo pipefail

# Configuração via variáveis de ambiente
LOKI_DATA_DIR="${LOKI_DATA_DIR:-/loki}"
MAX_SIZE_GB="${MAX_SIZE_GB:-5}"
CLEANUP_THRESHOLD_PERCENT="${CLEANUP_THRESHOLD_PERCENT:-80}"
CHECK_INTERVAL="${CHECK_INTERVAL:-300}"
LOKI_API_URL="${LOKI_API_URL:-http://loki:3100}"
METRICS_OUTPUT_DIR="${METRICS_OUTPUT_DIR:-/tmp/monitoring}"

# Criar diretório de métricas
mkdir -p "$METRICS_OUTPUT_DIR"

# Função para logging
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $*"
}

# Função para converter bytes para GB
bytes_to_gb() {
    echo "scale=2; $1 / 1024 / 1024 / 1024" | bc -l
}

# Função para verificar se Loki está saudável
is_loki_healthy() {
    curl -s -f "$LOKI_API_URL/ready" >/dev/null 2>&1
}

# Função para executar limpeza
trigger_cleanup() {
    local start_date=$(date -d '7 days ago' --iso-8601)
    local end_date=$(date -d '3 days ago' --iso-8601)
    
    log "Triggering cleanup for period: $start_date to $end_date"
    
    # Executar limpeza via API
    local cleanup_response=$(curl -s -X POST "$LOKI_API_URL/loki/api/v1/delete" \
        -H "Content-Type: application/json" \
        -d "{
            \"query\": \"{job=\\\"container_monitoring\\\"}\",
            \"start\": \"${start_date}T00:00:00Z\",
            \"end\": \"${end_date}T23:59:59Z\"
        }" 2>/dev/null || echo "FAILED")
    
    if [[ "$cleanup_response" != "FAILED" ]]; then
        log "Cleanup API response: $cleanup_response"
        
        # Forçar compactação
        curl -s -X POST "$LOKI_API_URL/compactor/ring" >/dev/null 2>&1 || true
        log "Compaction triggered"
        
        return 0
    else
        log "ERROR: Cleanup failed"
        return 1
    fi
}

# Função para atualizar métricas
update_metrics() {
    local current_size_bytes=$1
    local current_size_gb=$(bytes_to_gb "$current_size_bytes")
    local max_size_bytes=$((MAX_SIZE_GB * 1024 * 1024 * 1024))
    local usage_percent=$(echo "scale=2; $current_size_bytes * 100 / $max_size_bytes" | bc -l)
    
    # Salvar métricas em arquivo
    cat > "$METRICS_OUTPUT_DIR/loki_metrics.prom" << EOF
# HELP loki_data_size_bytes Total size of Loki data in bytes
# TYPE loki_data_size_bytes gauge
loki_data_size_bytes $current_size_bytes

# HELP loki_data_size_gb Total size of Loki data in GB
# TYPE loki_data_size_gb gauge
loki_data_size_gb $current_size_gb

# HELP loki_max_size_bytes Maximum allowed size of Loki data in bytes
# TYPE loki_max_size_bytes gauge
loki_max_size_bytes $max_size_bytes

# HELP loki_usage_percent Current usage percentage of Loki data
# TYPE loki_usage_percent gauge
loki_usage_percent $usage_percent

# HELP loki_metrics_available Indicates if Loki metrics are available
# TYPE loki_metrics_available gauge
loki_metrics_available 1
EOF
}

# Loop principal de monitoramento
main_loop() {
    log "Starting Loki storage monitoring loop"
    log "Data directory: $LOKI_DATA_DIR"
    log "Max size: ${MAX_SIZE_GB}GB"
    log "Cleanup threshold: ${CLEANUP_THRESHOLD_PERCENT}%"
    
    while true; do
        # Verificar se o diretório existe
        if [[ ! -d "$LOKI_DATA_DIR" ]]; then
            log "ERROR: Loki data directory not found: $LOKI_DATA_DIR"
            sleep "$CHECK_INTERVAL"
            continue
        fi
        
        # Obter tamanho atual
        local current_size_bytes=$(du -sb "$LOKI_DATA_DIR" 2>/dev/null | cut -f1)
        if [[ -z "$current_size_bytes" ]]; then
            log "ERROR: Could not determine directory size"
            sleep "$CHECK_INTERVAL"
            continue
        fi
        
        local current_size_gb=$(bytes_to_gb "$current_size_bytes")
        local max_size_bytes=$((MAX_SIZE_GB * 1024 * 1024 * 1024))
        local threshold_bytes=$((max_size_bytes * CLEANUP_THRESHOLD_PERCENT / 100))
        local usage_percent=$(echo "scale=1; $current_size_bytes * 100 / $max_size_bytes" | bc -l)
        
        log "Current size: ${current_size_gb}GB (${usage_percent}%)"
        
        # Atualizar métricas
        update_metrics "$current_size_bytes"
        
        # Verificar se precisa de limpeza
        if [[ "$current_size_bytes" -gt "$threshold_bytes" ]]; then
            log "WARNING: Storage threshold exceeded"
            
            if is_loki_healthy; then
                if trigger_cleanup; then
                    log "Cleanup completed successfully"
                    # Aguardar limpeza fazer efeito
                    sleep 60
                else
                    log "ERROR: Cleanup failed"
                fi
            else
                log "ERROR: Loki is not healthy, skipping cleanup"
            fi
        else
            log "Storage usage within acceptable limits"
        fi
        
        sleep "$CHECK_INTERVAL"
    done
}

# Tratamento de sinais
trap 'log "Monitor stopped"; exit 0' SIGTERM SIGINT

# Iniciar monitoramento
main_loop
