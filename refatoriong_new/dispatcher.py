"""
dispatcher.py - Fluxo principal de roteamento de logs para sinks.
- Recebe linhas de log de containers ou arquivos.
- Processa (pipeline) e adiciona labels conforme tipo de origem.
- Envia para sinks configurados (Loki, Elasticsearch, Splunk, arquivo local).
- Aplica fallback robusto para DLQ em caso de erro.
"""

import time
import json
from datetime import datetime
from typing import Dict, Optional
from .metrics import metrics
from .robustness import StructuredLogger
from .config import LOKI_SINK_ENABLED, ELASTICSEARCH_SINK_ENABLED, SPLUNK_SINK_ENABLED, LOCAL_SINK_ENABLED
from .config import SELF_ID_SHORT, SELF_CONTAINER_NAME, SELF_FEEDBACK_GUARD
# NOVO: limites de drift de timestamp
from .config import TIMESTAMP_MAX_PAST_AGE_SECONDS, TIMESTAMP_MAX_FUTURE_AGE_SECONDS
# NOVO: toggle do clamp
from .config import TIMESTAMP_CLAMP_ENABLED

class LogDispatcher:
    """
    LogDispatcher - Roteia logs processados para os sinks.
    - Adiciona labels 'job' conforme tipo de origem.
    - Aplica fallback robusto para DLQ.
    - Métricas detalhadas.
    """
    def __init__(self, sinks: Dict[str, object], processor: Optional[object] = None):
        self.sinks = sinks
        self.processor = processor
        self.logger = StructuredLogger("dispatcher")

    def _is_self_source(self, source_type: str, source_id: str, base_labels: Dict) -> bool:
        if not SELF_FEEDBACK_GUARD:
            return False
        if source_type != "docker":
            return False
        if source_id == SELF_ID_SHORT:
            return True
        if base_labels.get("container_name") == SELF_CONTAINER_NAME:
            return True
        return False

    async def handle(self, source_type: str, source_id: str, line: str, base_labels: Dict):
        """
        Recebe uma linha de log, processa e envia para os sinks.
        - Adiciona label 'job' explícita.
        - Fallback para labels padrão se necessário.
        - Envia para todos os sinks habilitados.
        - Em caso de erro, envia para DLQ local.
        """
        processed_data = {}
        try:
            # Processamento pipeline (se configurado)
            if self.processor:
                source_name = base_labels.get("pipeline") or base_labels.get("container_name", source_id)
                processed_data = await self.processor.process(source_type, source_id, source_name, line)
            processed_labels = processed_data.get('labels', {})
            log_fields = processed_data.get('fields', {})

            # Labels finais: base + processadas
            final_labels = {**base_labels, **processed_labels}

            # Lógica explícita de labels 'job'
            if source_type == "docker":
                final_labels["job"] = "container_monitoring"
            elif source_type == "file":
                final_labels["job"] = "file_monitoring"
            else:
                final_labels.setdefault("job", "generic_monitoring")

            # Fallback: adiciona campos base como fields se não estiverem nas labels
            for k, v in base_labels.items():
                if k not in final_labels:
                    log_fields[k] = v

            # Fallback: sempre garantir campo 'message'
            if 'message' not in log_fields:
                log_fields['message'] = line

            # Timestamp: usa do campo ou atual
            log_ts = int(time.time() * 1e9)
            if 'timestamp' in log_fields:
                try:
                    ts_str = log_fields.pop('timestamp')
                    dt = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    log_ts = int(dt.timestamp() * 1e9)
                except Exception:
                    if not self._is_self_source(source_type, source_id, base_labels):
                        self.logger.warning("Falha ao converter timestamp, usando atual", ts_str=ts_str)
                    else:
                        self.logger.debug("Falha ao converter timestamp (self) - suprimido", ts_str=ts_str)

            # NOVO: clamp de timestamp (passado/futuro) para evitar rejeição no Loki
            now_ns = int(time.time() * 1e9)
            max_past_ns = TIMESTAMP_MAX_PAST_AGE_SECONDS * 1_000_000_000
            max_future_ns = TIMESTAMP_MAX_FUTURE_AGE_SECONDS * 1_000_000_000
            if TIMESTAMP_CLAMP_ENABLED:
                if log_ts < now_ns - max_past_ns:
                    if not self._is_self_source(source_type, source_id, base_labels):
                        self.logger.warning("Timestamp muito antigo; ajustando para agora", ts_original=log_ts, drift_seconds=int((now_ns - log_ts)/1e9))
                    log_ts = now_ns
                elif log_ts > now_ns + max_future_ns:
                    if not self._is_self_source(source_type, source_id, base_labels):
                        self.logger.warning("Timestamp no futuro; ajustando para agora", ts_original=log_ts, drift_seconds=int((log_ts - now_ns)/1e9))
                    log_ts = now_ns
            else:
                # Ajuste desabilitado via configuração; preservar timestamp original
                self.logger.debug("Clamp de timestamp desabilitado por configuração", clamp_enabled=False)

            record = {
                "ts": log_ts,
                "line": json.dumps(log_fields),
                "labels": final_labels,
                "meta": {"source_type": source_type, "source_id": source_id}
            }

            # Envio para sinks habilitados
            try:
                if LOCAL_SINK_ENABLED and "localfile" in self.sinks:
                    await self.sinks["localfile"].send(record)
                if LOKI_SINK_ENABLED and "loki" in self.sinks:
                    await self.sinks["loki"].send(record)
                if ELASTICSEARCH_SINK_ENABLED and "elasticsearch" in self.sinks:
                    await self.sinks["elasticsearch"].send(record)
                if SPLUNK_SINK_ENABLED and "splunk" in self.sinks:
                    await self.sinks["splunk"].send(record)
                source_name_label = base_labels.get("container_name", base_labels.get("filepath", "unknown"))
                metrics.LINES_PROCESSED.labels(source_type=source_type, source_id=source_id, source_name=source_name_label).inc()
                metrics.BYTES_PROCESSED.labels(source_type=source_type, source_id=source_id, source_name=source_name_label).inc(len(line))
            except Exception as e:
                # Fallback robusto: DLQ local
                record['meta']['is_dlq'] = True
                record['meta']['dlq_reason'] = str(e)
                if "localfile" in self.sinks:
                    await self.sinks["localfile"].send(record)
                metrics.LOGS_DLQ.labels(source_type=source_type, reason="sink_error").inc()
                if not self._is_self_source(source_type, source_id, base_labels):
                    self.logger.error("Erro ao enviar para sink, enviado para DLQ", error=str(e))
                else:
                    self.logger.debug("Erro ao enviar para sink (self) - suprimido; enviado para DLQ", error=str(e))
        except Exception as e:
            # Fallback global: DLQ local
            dlq_record = {
                "ts": int(time.time() * 1e9),
                "line": line,
                "labels": base_labels,
                "meta": {
                    "source_type": source_type,
                    "source_id": source_id,
                    "is_dlq": True,
                    "dlq_reason": f"processing_error: {str(e)}"
                }
            }
            if "localfile" in self.sinks:
                await self.sinks["localfile"].send(dlq_record)
            metrics.LOGS_DLQ.labels(source_type=source_type, reason="processing_error").inc()
            if not self._is_self_source(source_type, source_id, base_labels):
                self.logger.error("Erro global no dispatcher, enviado para DLQ", error=str(e))
            else:
                self.logger.debug("Erro global no dispatcher (self) - suprimido; enviado para DLQ", error=str(e))

    async def start(self):
        """Inicializa dispatcher (placeholder)."""
        self.logger.info("Dispatcher iniciado")

    async def stop(self):
        """Finaliza dispatcher (placeholder)."""
        self.logger.info("Dispatcher parado")
