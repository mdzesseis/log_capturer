import re
import os
import json
import time
import uuid
import yaml
import asyncio
import traceback
from typing import Dict, List, Optional
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from .metrics import metrics
from .robustness import StructuredLogger, CorrelationContext
from .config import PIPELINE_CONFIG_FILE
# NOVO: flags de hot-path logging
from .config import HOTPATH_DEBUG_ENABLED, HOTPATH_DEBUG_SAMPLE_N
# NOVO: flags de otimização do pipeline
from .config import PIPELINE_OPTIMIZE_ORDER_ENABLED, PIPELINE_EARLY_EXIT_ENABLED

class PipelineStep:
    def __init__(self, name: str, pattern: str, step_type: str = "field", compiled_pattern: Optional[re.Pattern] = None, exclusive: bool = False):
        self.name = name
        # NOVO: usar pattern pré-compilado quando disponível
        self.compiled_pattern = compiled_pattern if compiled_pattern is not None else re.compile(pattern)
        self.step_type = step_type
        # NOVO: permite early-exit quando fizer match
        self.exclusive = bool(exclusive)
        self.logger = StructuredLogger("pipeline_step")
        # NOVO: contador de misses para amostragem de debug
        self._miss_counter = 0

    def apply(self, line: str) -> Dict[str, str]:
        m = self.compiled_pattern.search(line)
        if not m:
            # NOVO: debug amostrado no hot-path
            self._miss_counter += 1
            if HOTPATH_DEBUG_ENABLED and HOTPATH_DEBUG_SAMPLE_N > 0 and (self._miss_counter % HOTPATH_DEBUG_SAMPLE_N == 0):
                self.logger.debug("Regex sem match (amostrado)", step=self.name, misses=self._miss_counter)
            return {}
        if m.groupdict():
            return {k: v for k, v in m.groupdict().items() if v is not None}
        return {f'group_{i}': g for i, g in enumerate(m.groups(), 1)}

class PipelineConfig:
    def __init__(self):
        self.pipelines: Dict[str, List[Dict]] = {}
        self.source_mapping: Dict[str, str] = {}
        self.logger = StructuredLogger("pipeline_config")
        # NOVO: mapa de passos pré-compilados por pipeline
        self._compiled: Dict[str, List[Dict]] = {}

    @classmethod
    def load(cls, path: Path) -> 'PipelineConfig':
        cfg = cls()
        if not path.exists():
            cfg.logger.warning("Arquivo de pipelines não encontrado; usando fallback (JSON/KV autodetect)", path=str(path))
            return cfg
        with open(path, 'r') as f:
            data = yaml.safe_load(f) if path.suffix in ('.yaml','.yml') else json.load(f)
        if not data:
            cfg.logger.warning("Config de pipeline vazia; usando fallback (JSON/KV autodetect)", path=str(path))
            return cfg
        # Monta pipelines preservando compatibilidade (type: 'field' ou 'label')
        for name, steps in data.get('pipelines', {}).items():
            arr = []
            for s in steps or []:
                if isinstance(s, dict) and 'name' in s and 'pattern' in s:
                    arr.append({
                        "name": s['name'],
                        "pattern": s['pattern'],
                        "type": s.get('type', 'field')
                    })
                else:
                    cfg.logger.warning("Passo de pipeline inválido; ignorando", pipeline=name, step=str(s))
            cfg.pipelines[name] = arr
        cfg.source_mapping = data.get('source_mapping', {})
        # Validação básica
        cfg._validate()
        # NOVO: função de score de seletividade para ordenar heurísticamente
        def _score_pattern(pat: str) -> int:
            if not pat: return 0
            score = 0
            # Âncora de início/fim aumenta seletividade
            if pat.startswith("^"): score += 3
            if pat.endswith("$"): score += 2
            # Penaliza curingas muito amplos
            score -= 2 * pat.count(".*")
            # Beneficia literais longos (estimativa simples)
            literal = re.sub(r"[\\\[\]\(\)\?\+\*\{\}\|\^\$\.\-\s]", "", pat)
            score += min(len(literal) // 4, 5)
            return score

        # NOVO: pré-compilar padrões por pipeline com ordenação por seletividade
        total_steps = 0
        for p_name, steps in cfg.pipelines.items():
            # aplica ordenação se habilitada
            if PIPELINE_OPTIMIZE_ORDER_ENABLED:
                steps_sorted = sorted(steps, key=lambda s: _score_pattern(s.get("pattern", "")), reverse=True)
                if steps_sorted != steps:
                    cfg.logger.info("Pipeline reordenado por seletividade", pipeline=p_name, original=len(steps), reordered=len(steps_sorted))
                steps = steps_sorted
            compiled_steps = []
            for s in steps:
                try:
                    cp = re.compile(s.get("pattern", ""))
                    compiled_steps.append({
                        "name": s["name"],
                        "compiled": cp,
                        "type": s.get("type", "field"),
                        # NOVO: suporte a early-exit por passo
                        "exclusive": bool(s.get("exclusive", False)),
                    })
                    total_steps += 1
                except re.error as e:
                    cfg.logger.error("Falha ao compilar regex; passo ignorado", pipeline=p_name, step=s.get("name"), error=str(e))
            cfg._compiled[p_name] = compiled_steps
        cfg.logger.info("Pipelines pré-compilados", pipelines=len(cfg._compiled), total_steps=total_steps)
        return cfg

    def _validate(self):
        for p_name, steps in self.pipelines.items():
            for idx, s in enumerate(steps):
                if s.get("type") not in ("field", "label"):
                    # Mantém compatibilidade e alerta quando tipo desconhecido
                    self.logger.warning("Tipo de passo não suportado (usando como 'field')", pipeline=p_name, step=s.get("name"), type=s.get("type"))
                    s["type"] = "field"
                try:
                    re.compile(s.get("pattern", ""))
                except re.error as e:
                    self.logger.error("Regex inválida em passo de pipeline", pipeline=p_name, step=s.get("name"), error=str(e))

class LogProcessor:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.executor: Optional[ProcessPoolExecutor] = None
        self.logger = StructuredLogger("log_processor")
        # NOVO: cache composto por pipeline name -> lista de PipelineStep
        self._compiled_cache: Dict[str, List[PipelineStep]] = {}

    def start(self, workers: int):
        if workers > 0:
            self.executor = ProcessPoolExecutor(max_workers=workers)
    def stop(self):
        if self.executor:
            self.executor.shutdown()
    def get_pipeline(self, source_type: str, source_name: str) -> List[Dict]:
        key = f"{source_type}/{source_name}"
        if key in self.config.source_mapping:
            return self.config.pipelines.get(self.config.source_mapping[key], [])
        if source_type in self.config.source_mapping:
            return self.config.pipelines.get(self.config.source_mapping[source_type], [])
        if source_name in self.config.pipelines:
            return self.config.pipelines.get(source_name, [])
        return self.config.pipelines.get('default', [])
    def get_pipeline_config(self, source_type: str, source_name: str) -> List[Dict]:
        return self.get_pipeline(source_type, source_name)

    def _get_compiled_steps(self, source_type: str, source_name: str) -> List[PipelineStep]:
        p_steps = self.get_pipeline(source_type, source_name)
        # chave de cache: nome lógico do pipeline
        if source_name in self.config.pipelines:
            key = source_name
        elif source_type in self.config.source_mapping:
            key = self.config.source_mapping[source_type]
        elif f"{source_type}/{source_name}" in self.config.source_mapping:
            key = self.config.source_mapping[f"{source_type}/{source_name}"]
        else:
            key = "default"
        if key in self._compiled_cache:
            self.logger.debug("Pipeline reutilizado do cache", pipeline=key, steps=len(self._compiled_cache[key]))
            return self._compiled_cache[key]
        compiled_defs = self.config._compiled.get(key, [])
        steps = [PipelineStep(
                    s["name"],
                    pattern="",
                    step_type=s.get("type","field"),
                    compiled_pattern=s["compiled"],
                    exclusive=bool(s.get("exclusive", False))
                ) for s in compiled_defs]
        self._compiled_cache[key] = steps
        self.logger.info("Pipeline compilado em cache", pipeline=key, steps=len(steps), early_exit_enabled=PIPELINE_EARLY_EXIT_ENABLED, order_optimized=PIPELINE_OPTIMIZE_ORDER_ENABLED)
        return steps

    async def process(self, source_type: str, source_id: str, source_name: str, line: str) -> Dict[str, str]:
        start = time.perf_counter()
        pipeline_cfg = self.get_pipeline_config(source_type, source_name)
        self.logger.debug("Processando linha", source_type=source_type, source_id=source_id, source_name=source_name, pipeline_steps=len(pipeline_cfg))
        try:
            if pipeline_cfg:
                compiled_steps = self._get_compiled_steps(source_type, source_name)
                result = self._process_sync_compiled(compiled_steps, line)
            else:
                result = self._fallback_multi_format(line)
            return result
        except Exception as e:
            metrics.PROCESSING_ERRORS.labels(source_type=source_type, source_id=source_id).inc()
            self.logger.error("Erro no processamento da linha", error=str(e))
            return {}
        finally:
            duration = time.perf_counter() - start
            try:
                metrics.PROCESSING_TIME.labels(source_type=source_type, source_id=source_id).observe(duration)
            except Exception:
                pass

    @staticmethod
    def _process_sync_compiled(steps: List[PipelineStep], line: str) -> Dict[str, str]:
        results = {"labels": {}, "fields": {}}
        for step in steps:
            data = step.apply(line)
            if not data:
                continue
            if step.step_type == 'label':
                results['labels'].update(data)
            else:
                results['fields'].update(data)
            # NOVO: early-exit quando match em passo exclusivo
            if step.exclusive and PIPELINE_EARLY_EXIT_ENABLED:
                step.logger.debug("Early-exit aplicado após match", step=step.name)
                break
        return results

    def _fallback_multi_format(self, line: str) -> Dict[str, str]:
        """
        Suporte básico a múltiplos formatos:
        - JSON: parse completo das chaves
        - KV (key=value separados por espaço)
        """
        # Tenta JSON
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                self.logger.debug("Fallback JSON aplicado")
                return {"labels": {}, "fields": obj}
        except Exception:
            pass
        # Tenta KV
        fields: Dict[str, str] = {}
        try:
            for token in filter(None, (t.strip() for t in line.split())):
                if "=" in token:
                    k, v = token.split("=", 1)
                    if k:
                        fields[k] = v.strip().strip('"')
            if fields:
                self.logger.debug("Fallback KV aplicado", keys=list(fields.keys())[:5])
                return {"labels": {}, "fields": fields}
        except Exception:
            pass
        # Sem transformação
        return {"labels": {}, "fields": {"message": line}}
        # Tenta JSON
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                self.logger.debug("Fallback JSON aplicado")
                return {"labels": {}, "fields": obj}
        except Exception:
            pass
        # Tenta KV
        fields: Dict[str, str] = {}
        try:
            for token in filter(None, (t.strip() for t in line.split())):
                if "=" in token:
                    k, v = token.split("=", 1)
                    if k:
                        fields[k] = v.strip().strip('"')
            if fields:
                self.logger.debug("Fallback KV aplicado", keys=list(fields.keys())[:5])
                return {"labels": {}, "fields": fields}
        except Exception:
            pass
        # Sem transformação
        return {"labels": {}, "fields": {"message": line}}
