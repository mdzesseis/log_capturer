#!/bin/bash

# Script principal para executar o monitoramento
set -e

echo "🔍 Loki Storage Monitoring"
echo "=========================="

# Verificar se Docker está rodando
if ! docker info >/dev/null 2>&1; then
    echo "❌ Docker is not running"
    exit 1
fi

# Verificar se Loki está rodando
if ! docker ps | grep -q loki; then
    echo "❌ Loki container is not running"
    exit 1
fi

# Executar monitoramento
echo "📊 Running storage check..."
./scripts/monitor-loki-size.sh

# Mostrar métricas atuais
echo ""
echo "📈 Current metrics:"
if [ -f "/tmp/monitoring/loki_metrics.prom" ]; then
    grep -E "(loki_data_size_gb|loki_usage_percent)" /tmp/monitoring/loki_metrics.prom
else
    echo "No metrics file found"
fi

# Verificar alertas no Prometheus (se disponível)
echo ""
echo "🚨 Checking for active alerts..."
if curl -s "http://localhost:9090/api/v1/alerts" | grep -q "loki"; then
    echo "Active Loki alerts found in Prometheus"
else
    echo "No active Loki alerts"
fi

echo ""
echo "✅ Monitoring complete"
