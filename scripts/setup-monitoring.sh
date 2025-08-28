#!/bin/bash

# Script para configurar o monitoramento do Loki
echo "Setting up Loki monitoring..."

# Tornar script executável
chmod +x scripts/monitor-loki-size.sh

# Criar diretórios necessários
mkdir -p /tmp/monitoring
mkdir -p logs

# Testar dependências
echo "Checking dependencies..."

# Verificar bc (calculadora)
if ! command -v bc &> /dev/null; then
    echo "Installing bc (calculator)..."
    sudo apt-get update && sudo apt-get install -y bc
fi

# Verificar curl
if ! command -v curl &> /dev/null; then
    echo "Installing curl..."
    sudo apt-get update && sudo apt-get install -y curl
fi

# Testar script
echo "Testing monitoring script..."
./scripts/monitor-loki-size.sh

echo "Loki monitoring setup complete!"
echo ""
echo "Usage:"
echo "  Manual run:    ./scripts/monitor-loki-size.sh"
echo "  Setup cron:    ./scripts/setup-cron.sh"
echo "  View metrics:  cat /tmp/monitoring/loki_metrics.prom"
