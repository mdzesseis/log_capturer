#!/bin/bash

# Script para configurar cron job do monitoramento
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
MONITOR_SCRIPT="$PROJECT_DIR/scripts/monitor-loki-size.sh"

echo "Setting up cron job for Loki monitoring..."
echo "Script location: $MONITOR_SCRIPT"

# Verificar se o script existe
if [ ! -f "$MONITOR_SCRIPT" ]; then
    echo "ERROR: Monitor script not found at $MONITOR_SCRIPT"
    exit 1
fi

# Criar entrada do cron (executa a cada 15 minutos)
CRON_JOB="*/15 * * * * cd $PROJECT_DIR && $MONITOR_SCRIPT >> logs/loki-monitor.log 2>&1"

# Adicionar ao cron
(crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -

echo "Cron job added successfully!"
echo "The monitoring script will run every 15 minutes."
echo ""
echo "To view cron jobs: crontab -l"
echo "To remove cron job: crontab -e (then delete the line)"
echo "To view logs: tail -f logs/loki-monitor.log"

# Criar arquivo de log inicial
touch logs/loki-monitor.log
echo "Log file created: logs/loki-monitor.log"
