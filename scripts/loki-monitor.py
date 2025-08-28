#!/usr/bin/env python3
"""
Loki Storage Monitor - Container Version
Monitora o uso de espaço do Loki e executa limpeza automática quando necessário.
"""

import os
import time
import json
import logging
import requests
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
from prometheus_client import Gauge, Counter, start_http_server
from prometheus_client.core import CollectorRegistry

# Configuração via variáveis de ambiente
LOKI_DATA_DIR = os.getenv("LOKI_DATA_DIR", "/loki")
MAX_SIZE_GB = float(os.getenv("MAX_SIZE_GB", "5"))
CLEANUP_THRESHOLD_PERCENT = int(os.getenv("CLEANUP_THRESHOLD_PERCENT", "80"))
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "300"))  # 5 minutos
LOKI_API_URL = os.getenv("LOKI_API_URL", "http://loki:3100")
METRICS_OUTPUT_DIR = os.getenv("METRICS_OUTPUT_DIR", "/tmp/monitoring")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9091"))

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Métricas Prometheus
registry = CollectorRegistry()
loki_data_size_bytes = Gauge('loki_data_size_bytes', 'Total size of Loki data in bytes', registry=registry)
loki_data_size_gb = Gauge('loki_data_size_gb', 'Total size of Loki data in GB', registry=registry)
loki_max_size_bytes = Gauge('loki_max_size_bytes', 'Maximum allowed size of Loki data in bytes', registry=registry)
loki_usage_percent = Gauge('loki_usage_percent', 'Current usage percentage of Loki data', registry=registry)
loki_cleanup_operations = Counter('loki_cleanup_operations_total', 'Total cleanup operations performed', registry=registry)
loki_cleanup_errors = Counter('loki_cleanup_errors_total', 'Total cleanup errors', registry=registry)

class LokiStorageMonitor:
    def __init__(self):
        self.data_dir = Path(LOKI_DATA_DIR)
        self.max_size_bytes = int(MAX_SIZE_GB * 1024 * 1024 * 1024)
        self.threshold_bytes = int(self.max_size_bytes * CLEANUP_THRESHOLD_PERCENT / 100)
        self.metrics_dir = Path(METRICS_OUTPUT_DIR)
        
        # CORRIGIDO: Criar diretório com verificação de permissões
        try:
            self.metrics_dir.mkdir(parents=True, exist_ok=True)
            # Testar escrita no diretório
            test_file = self.metrics_dir / "test_write.tmp"
            test_file.write_text("test")
            test_file.unlink()
            logger.info(f"Metrics directory verified: {self.metrics_dir}")
        except Exception as e:
            logger.error(f"Cannot write to metrics directory {self.metrics_dir}: {e}")
            # Fallback para diretório local
            self.metrics_dir = Path("/tmp/loki_metrics")
            self.metrics_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Using fallback metrics directory: {self.metrics_dir}")
        
        logger.info(f"Initialized Loki Storage Monitor")
        logger.info(f"Data directory: {self.data_dir}")
        logger.info(f"Max size: {MAX_SIZE_GB}GB")
        logger.info(f"Cleanup threshold: {CLEANUP_THRESHOLD_PERCENT}%")
        
    def get_directory_size(self) -> int:
        """Calcula o tamanho total do diretório de dados do Loki"""
        try:
            total_size = 0
            for path in self.data_dir.rglob('*'):
                if path.is_file():
                    total_size += path.stat().st_size
            return total_size
        except Exception as e:
            logger.error(f"Error calculating directory size: {e}")
            return 0
    
    def bytes_to_gb(self, bytes_value: int) -> float:
        """Converte bytes para GB"""
        return round(bytes_value / (1024 * 1024 * 1024), 2)
    
    def is_loki_healthy(self) -> bool:
        """Verifica se o Loki está respondendo"""
        try:
            response = requests.get(f"{LOKI_API_URL}/ready", timeout=10)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Loki health check failed: {e}")
            return False
    
    def trigger_cleanup(self) -> bool:
        """Executa limpeza via API do Loki"""
        try:
            if not self.is_loki_healthy():
                logger.error("Loki is not healthy, skipping cleanup")
                return False
            
            # Calcular datas para limpeza (logs mais antigos que 3 dias)
            end_date = datetime.now() - timedelta(days=3)
            start_date = datetime.now() - timedelta(days=7)
            
            cleanup_payload = {
                "query": "{job=\"container_monitoring\"}",
                "start": start_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "end": end_date.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
            
            logger.info(f"Triggering cleanup for period: {start_date} to {end_date}")
            
            # Executar limpeza
            cleanup_response = requests.post(
                f"{LOKI_API_URL}/loki/api/v1/delete",
                json=cleanup_payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            
            if cleanup_response.status_code in [200, 202, 204]:
                logger.info(f"Cleanup API response: {cleanup_response.status_code}")
                
                # Forçar compactação
                try:
                    compaction_response = requests.post(
                        f"{LOKI_API_URL}/compactor/ring",
                        timeout=30
                    )
                    logger.info(f"Compaction triggered: {compaction_response.status_code}")
                except Exception as e:
                    logger.warning(f"Compaction trigger failed: {e}")
                
                loki_cleanup_operations.inc()
                return True
            else:
                logger.error(f"Cleanup failed: {cleanup_response.status_code} - {cleanup_response.text}")
                loki_cleanup_errors.inc()
                return False
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
            loki_cleanup_errors.inc()
            return False
    
    def update_metrics(self, current_size_bytes: int):
        """Atualiza métricas Prometheus"""
        current_size_gb = self.bytes_to_gb(current_size_bytes)
        usage_percent = (current_size_bytes / self.max_size_bytes) * 100 if self.max_size_bytes > 0 else 0
        
        # Atualizar métricas Prometheus
        loki_data_size_bytes.set(current_size_bytes)
        loki_data_size_gb.set(current_size_gb)
        loki_max_size_bytes.set(self.max_size_bytes)
        loki_usage_percent.set(usage_percent)
        
        # Salvar métricas em arquivo para coleta externa
        metrics_content = f"""# HELP loki_data_size_bytes Total size of Loki data in bytes
# TYPE loki_data_size_bytes gauge
loki_data_size_bytes {current_size_bytes}

# HELP loki_data_size_gb Total size of Loki data in GB
# TYPE loki_data_size_gb gauge
loki_data_size_gb {current_size_gb}

# HELP loki_max_size_bytes Maximum allowed size of Loki data in bytes
# TYPE loki_max_size_bytes gauge
loki_max_size_bytes {self.max_size_bytes}

# HELP loki_usage_percent Current usage percentage of Loki data
# TYPE loki_usage_percent gauge
loki_usage_percent {usage_percent:.2f}

# HELP loki_metrics_available Indicates if Loki metrics are available
# TYPE loki_metrics_available gauge
loki_metrics_available 1
"""
        
        try:
            metrics_file = self.metrics_dir / "loki_metrics.prom"
            with open(metrics_file, 'w') as f:
                f.write(metrics_content)
            logger.debug(f"Metrics updated: {current_size_gb}GB ({usage_percent:.1f}%)")
        except Exception as e:
            logger.error(f"Error saving metrics file: {e}")
    
    def monitor_loop(self):
        """Loop principal de monitoramento"""
        logger.info("Starting Loki storage monitoring loop")
        
        while True:
            try:
                # Obter tamanho atual
                current_size_bytes = self.get_directory_size()
                current_size_gb = self.bytes_to_gb(current_size_bytes)
                usage_percent = (current_size_bytes / self.max_size_bytes) * 100 if self.max_size_bytes > 0 else 0
                
                logger.info(f"Current size: {current_size_gb}GB ({usage_percent:.1f}%)")
                
                # Atualizar métricas
                self.update_metrics(current_size_bytes)
                
                # Verificar se precisa de limpeza
                if current_size_bytes > self.threshold_bytes:
                    logger.warning(f"Storage threshold exceeded: {current_size_gb}GB > {self.bytes_to_gb(self.threshold_bytes)}GB")
                    
                    if self.trigger_cleanup():
                        logger.info("Cleanup completed successfully")
                        # Aguardar um pouco para a limpeza fazer efeito
                        time.sleep(60)
                    else:
                        logger.error("Cleanup failed")
                else:
                    logger.info("Storage usage within acceptable limits")
                
            except Exception as e:
                logger.error(f"Error in monitoring loop: {e}")
            
            # Aguardar próxima verificação
            time.sleep(CHECK_INTERVAL)

def main():
    """Função principal"""
    try:
        # Iniciar servidor de métricas Prometheus
        start_http_server(METRICS_PORT, registry=registry)
        logger.info(f"Prometheus metrics server started on port {METRICS_PORT}")
        
        # Iniciar monitor
        monitor = LokiStorageMonitor()
        monitor.monitor_loop()
        
    except KeyboardInterrupt:
        logger.info("Monitor stopped by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
