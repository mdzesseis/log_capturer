"""
task_manager.py - Gerenciamento de tarefas assíncronas com recovery.
- Registro, inicialização e monitoramento de tarefas de longa duração.
- Reinício automático em caso de falha com backoff exponencial.
- Circuit breaker para evitar retry storm.
- Health check periódico para tarefas críticas.
"""

import time
import asyncio
import logging
import traceback
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any, Awaitable
from datetime import datetime, timedelta

from .metrics import metrics
from .circuit_breaker import CircuitBreakerState
from .robustness import StructuredLogger, CorrelationContext
from .config import (
    TASK_RESTART_ENABLED, TASK_MAX_RESTART_COUNT,
    TASK_RESTART_BACKOFF_BASE, TASK_HEALTH_CHECK_INTERVAL,
    CIRCUIT_BREAKER_ENABLED, CIRCUIT_BREAKER_FAILURE_THRESHOLD,
    TASK_MAX_RESTARTS_PER_HOUR, TASK_CIRCUIT_BREAKER_DURATION,
    ORPHANED_TASK_CLEANUP_INTERVAL
)

class TaskPriority(Enum):
    """Prioridade da tarefa para ordem de startup e recursos."""
    CRITICAL = auto()
    HIGH = auto()
    NORMAL = auto()
    LOW = auto()
    BACKGROUND = auto()

@dataclass
class TaskInfo:
    """Metadados e estado de uma tarefa gerenciada."""
    task_id: str
    group: str
    callback: Callable
    priority: TaskPriority = TaskPriority.NORMAL
    kwargs: Dict[str, Any] = field(default_factory=dict)
    running: bool = False
    restarts: int = 0
    last_restart: float = 0
    last_heartbeat: float = 0
    state: str = "initialized"
    labels: Dict[str, str] = field(default_factory=dict)
    restart_timestamps: List[float] = field(default_factory=list)

class TaskManager:
    """
    Gerenciador de tarefas assíncronas com resilience patterns.
    - Reinício automático com backoff exponencial.
    - Circuit breaker para evitar retry storm.
    - Health check periódico para detectar tarefas "zombie".
    - Agrupamento para controle de dependências.
    """
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.task_info: Dict[str, TaskInfo] = {}
        self.health_status: Dict[str, Dict[str, Any]] = {}
        self.stop_event = asyncio.Event()
        self.logger = StructuredLogger("task_manager")
        self._health_check_task = None
        self._orphaned_task_cleanup = None

    async def start(self):
        """Inicia o health check periódico."""
        self.logger.info("TaskManager inicializando")
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        self._orphaned_task_cleanup = asyncio.create_task(self._orphaned_task_cleanup_loop())

    async def stop(self):
        """Para todas as tarefas gerenciadas e o health check."""
        self.logger.info("TaskManager finalizando")
        self.stop_event.set()
        
        # Cancela health check
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # Cancela limpeza de órfãos
        if self._orphaned_task_cleanup:
            self._orphaned_task_cleanup.cancel()
            try:
                await self._orphaned_task_cleanup
            except asyncio.CancelledError:
                pass

        # Cancela todas as tarefas gerenciadas
        cancel_tasks = []
        for name, task in list(self.tasks.items()):
            try:
                if not task.done():
                    task.cancel()
                    cancel_tasks.append(task)
            except Exception as e:
                self.logger.error("Erro ao cancelar tarefa", task_name=name, error=str(e))

        # Aguarda cancelamento
        if cancel_tasks:
            await asyncio.gather(*cancel_tasks, return_exceptions=True)

    def register_task(self, task_id: str, callback: Callable, group: str = "default", priority: TaskPriority = TaskPriority.NORMAL, **kwargs):
        """
        Registra uma nova tarefa para gerenciamento.
        - task_id: Identificador único da tarefa
        - callback: Função assíncrona a ser executada
        - group: Grupo para organização e dependências
        - priority: Prioridade da tarefa
        - **kwargs: Parâmetros adicionais para o callback
        """
        self.logger.debug("Registrando tarefa", task_id=task_id, group=group, priority=str(priority))
        self.task_info[task_id] = TaskInfo(
            task_id=task_id,
            group=group,
            callback=callback,
            priority=priority,
            kwargs=kwargs
        )
        self.health_status[task_id] = {
            "status": "registered",
            "group": group,
            "priority": str(priority),
            "registered_at": datetime.utcnow().isoformat()
        }
        metrics.TASK_HEALTH.labels(task_name=task_id).set(0)

    def set_task_labels(self, task_id: str, labels: Dict[str, str]):
        """Define labels para a tarefa (para métricas)."""
        if task_id in self.task_info:
            self.task_info[task_id].labels = labels
            self.health_status[task_id]["labels"] = labels

    async def start_task(self, task_id: str):
        """Inicia uma tarefa registrada."""
        if task_id not in self.task_info:
            self.logger.error("Tentativa de iniciar tarefa não registrada", task_id=task_id)
            return False
        
        info = self.task_info[task_id]
        if task_id in self.tasks and not self.tasks[task_id].done():
            self.logger.warning("Tarefa já em execução", task_id=task_id)
            return False

        # Gerar correlation_id transitório para o log de start (melhora rastreio; a task terá seu próprio ID no wrapper)
        try:
            transient_corr = CorrelationContext.generate_correlation_id()
            CorrelationContext.set_correlation_id(transient_corr)
        except Exception:
            transient_corr = None

        # Cria e inicia a tarefa
        self.tasks[task_id] = asyncio.create_task(
            self._task_wrapper(task_id, info.callback, **info.kwargs)
        )

        # Remover correlation_id transitório do contexto atual para evitar vazamento
        try:
            t = asyncio.current_task()
            if t:
                CorrelationContext._context.pop(id(t), None)
        except Exception:
            pass
        
        info.running = True
        info.state = "running"
        info.last_heartbeat = time.time()
        
        # Atualiza status
        self.health_status[task_id].update({
            "status": "running",
            "started_at": datetime.utcnow().isoformat(),
            "restarts": info.restarts
        })
        metrics.TASK_HEALTH.labels(task_name=task_id).set(1)
        
        self.logger.info("Tarefa iniciada", task_id=task_id, group=info.group)
        return True

    async def restart_task(self, task_id: str):
        """Reinicia uma tarefa com backoff exponencial."""
        if not TASK_RESTART_ENABLED:
            self.logger.debug("Reinício de tarefas desabilitado por configuração", task_id=task_id)
            return False

        if task_id not in self.task_info:
            self.logger.error("Tentativa de reiniciar tarefa não registrada", task_id=task_id)
            return False

        info = self.task_info[task_id]
        
        # Registra timestamp para controle de rate limit
        now = time.time()
        info.restart_timestamps.append(now)
        # Limpa timestamps antigos (> 1h)
        cutoff = now - 3600  # 1 hora
        info.restart_timestamps = [ts for ts in info.restart_timestamps if ts > cutoff]
        
        # Verifica rate limit
        if len(info.restart_timestamps) > TASK_MAX_RESTARTS_PER_HOUR:
            self.logger.warning(
                "Taxa de reinícios excedida, ativando circuit breaker",
                task_id=task_id,
                restarts_last_hour=len(info.restart_timestamps),
                max_allowed=TASK_MAX_RESTARTS_PER_HOUR
            )
            
            # Atualiza estado
            info.state = "circuit_open"
            self.health_status[task_id]["status"] = "circuit_open"
            self.health_status[task_id]["circuit_open_at"] = datetime.utcnow().isoformat()
            self.health_status[task_id]["circuit_recovery_at"] = (
                datetime.utcnow() + timedelta(seconds=TASK_CIRCUIT_BREAKER_DURATION)
            ).isoformat()
            
            metrics.TASK_HEALTH.labels(task_name=task_id).set(0)
            return False

        # Verifica limite de reinícios
        if info.restarts >= TASK_MAX_RESTART_COUNT:
            self.logger.error(
                "Limite máximo de reinícios atingido",
                task_id=task_id,
                restarts=info.restarts,
                max_restarts=TASK_MAX_RESTART_COUNT
            )
            # Atualiza estado
            info.state = "failed"
            self.health_status[task_id]["status"] = "failed"
            metrics.TASK_HEALTH.labels(task_name=task_id).set(0)
            return False

        # Calcula backoff exponencial
        backoff = TASK_RESTART_BACKOFF_BASE ** min(info.restarts, 8)  # max ~4 minutos com base 2
        self.logger.info(
            "Reiniciando tarefa com backoff",
            task_id=task_id,
            restarts=info.restarts + 1,
            backoff_seconds=backoff
        )

        # Incrementa contador de reinícios
        info.restarts += 1
        info.last_restart = time.time()
        
        # Atualiza métricas de restart
        container_name = info.labels.get("container_name", "unknown")
        metrics.TASK_RESTARTS.labels(task_name=task_id, container_name=container_name).inc()
        
        # Atualiza status
        self.health_status[task_id].update({
            "status": "restarting",
            "restarts": info.restarts,
            "backoff_seconds": backoff,
            "restart_at": datetime.utcnow().isoformat()
        })
        
        # Aguarda backoff e reinicia
        await asyncio.sleep(backoff)
        return await self.start_task(task_id)

    async def heartbeat(self, task_id: str):
        """
        Atualiza timestamp de último heartbeat da tarefa.
        Usado para detectar tarefas "zombie" (travadas).
        """
        if task_id in self.task_info:
            self.task_info[task_id].last_heartbeat = time.time()
            return True
        return False

    def get_task_status(self) -> Dict[str, Dict[str, Any]]:
        """Retorna status atual de todas as tarefas."""
        # Atualiza estado das tarefas
        for task_id, task in self.tasks.items():
            if task_id in self.health_status:
                if task.done():
                    if task.cancelled():
                        self.health_status[task_id]["status"] = "cancelled"
                    else:
                        try:
                            exc = task.exception()
                            if exc:
                                self.health_status[task_id]["status"] = "failed"
                                self.health_status[task_id]["error"] = str(exc)
                        except asyncio.CancelledError:
                            self.health_status[task_id]["status"] = "cancelled"
                        except asyncio.InvalidStateError:
                            # Resultado disponível, não exceção
                            self.health_status[task_id]["status"] = "completed"
                else:
                    self.health_status[task_id]["status"] = "running"
        
        # Detecta tarefas em circuit breaker com tempo de recovery
        now = datetime.utcnow()
        for task_id, status in self.health_status.items():
            if status.get("status") == "circuit_open" and task_id in self.task_info:
                circuit_open_at = status.get("circuit_open_at")
                if circuit_open_at:
                    try:
                        open_time = datetime.fromisoformat(circuit_open_at)
                        recovery_time = open_time + timedelta(seconds=TASK_CIRCUIT_BREAKER_DURATION)
                        if now >= recovery_time:
                            # Redefine estado para permitir nova tentativa
                            status["status"] = "pending_restart"
                            self.task_info[task_id].restart_timestamps = []
                            self.task_info[task_id].state = "pending_restart"
                    except Exception:
                        pass
        
        return self.health_status

    async def _task_wrapper(self, task_id: str, callback: Callable, **kwargs):
        """Wrapper para execução da tarefa com tratamento de erros e reinício."""
        # Garantir correlation_id por tarefa (isolamento por asyncio.Task)
        try:
            corr_id = CorrelationContext.generate_correlation_id()
            CorrelationContext.set_correlation_id(corr_id)
        except Exception:
            # não bloquear a execução se algo falhar aqui
            pass
        
        try:
            self.logger.debug("Iniciando wrapper da tarefa", task_id=task_id)
            
            # Executa o callback da tarefa
            await callback(**kwargs)
            
            # Callback terminou normalmente
            self.logger.info("Tarefa concluída normalmente", task_id=task_id)
            self.task_info[task_id].state = "completed"
            self.health_status[task_id]["status"] = "completed"
            self.health_status[task_id]["completed_at"] = datetime.utcnow().isoformat()
            
        except asyncio.CancelledError:
            # Tarefa cancelada explicitamente (não é erro)
            self.logger.info("Tarefa cancelada", task_id=task_id)
            self.task_info[task_id].state = "cancelled"
            self.health_status[task_id]["status"] = "cancelled"
            self.health_status[task_id]["cancelled_at"] = datetime.utcnow().isoformat()
            
        except Exception as e:
            # Erro não tratado na tarefa
            self.logger.error(
                "Erro na execução da tarefa",
                task_id=task_id,
                error=str(e),
                traceback=traceback.format_exc()
            )
            
            self.task_info[task_id].state = "error"
            self.health_status[task_id]["status"] = "error"
            self.health_status[task_id]["error"] = str(e)
            self.health_status[task_id]["error_at"] = datetime.utcnow().isoformat()
            
            # Tenta reiniciar a tarefa
            await self.restart_task(task_id)
            
        finally:
            # Limpeza: remover contexto desta task para evitar vazamento imediato
            try:
                t = asyncio.current_task()
                if t:
                    CorrelationContext._context.pop(id(t), None)
            except Exception:
                pass
            # Remove referência à tarefa (GC)
            self.task_info[task_id].running = False
            if task_id in self.tasks:
                try:
                    # Mantém a tarefa para permitir inspeção posterior (será substituída em restart)
                    pass
                except Exception:
                    pass

    async def _health_check_loop(self):
        """
        Loop periódico para verificar saúde das tarefas.
        - Detecta tarefas "zombie" (sem heartbeat)
        - Verifica circuit breakers para recovery
        - Tenta reiniciar tarefas pendentes
        """
        self.logger.info("Iniciando health check loop")
        
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(TASK_HEALTH_CHECK_INTERVAL)
                
                now = time.time()
                restart_candidates = []
                
                # Verifica todas as tarefas em execução
                for task_id, info in list(self.task_info.items()):
                    # Pula tarefas não iniciadas
                    if not info.running:
                        continue
                    
                    # Verifica se tem heartbeat recente
                    if (now - info.last_heartbeat) > (TASK_HEALTH_CHECK_INTERVAL * 1.5):
                        self.logger.warning(
                            "Tarefa sem heartbeat recente",
                            task_id=task_id,
                            seconds_since_heartbeat=now - info.last_heartbeat
                        )
                        
                        # Marca para restart
                        restart_candidates.append(task_id)
                
                # Tenta reiniciar tarefas sem heartbeat
                for task_id in restart_candidates:
                    self.logger.info("Tentando reiniciar tarefa sem heartbeat", task_id=task_id)
                    
                    # Cancela tarefa atual
                    if task_id in self.tasks and not self.tasks[task_id].done():
                        self.tasks[task_id].cancel()
                        try:
                            await asyncio.wait_for(self.tasks[task_id], timeout=5.0)
                        except (asyncio.TimeoutError, asyncio.CancelledError):
                            pass
                    
                    # Reinicia
                    await self.restart_task(task_id)
                
                # Tenta reiniciar tarefas em circuito aberto com tempo de recuperação
                status_dict = self.get_task_status()  # Isso já atualiza estados de circuito
                for task_id, status in status_dict.items():
                    if status.get("status") == "pending_restart" and task_id in self.task_info:
                        self.logger.info(
                            "Tentando reiniciar tarefa após circuit breaker",
                            task_id=task_id
                        )
                        # Reseta contadores e tenta reiniciar
                        info = self.task_info[task_id]
                        info.restart_timestamps = []
                        info.state = "pending_restart"
                        await self.start_task(task_id)
                
            except asyncio.CancelledError:
                self.logger.info("Health check loop cancelado")
                break
            except Exception as e:
                self.logger.error(
                    "Erro no health check loop",
                    error=str(e),
                    traceback=traceback.format_exc()
                )
    
    async def _is_container_active(self, container_id: str) -> bool:
        """
        Verifica se um container está ativo usando API Docker.
        Protege contra falsos positivos na detecção de órfãos.
        """
        try:
            from aiohttp import ClientSession, ClientTimeout
            from .config import DOCKER_SOCKET_URL
            
            # Adapta URL para funcionar com unix sockets também
            api_url = DOCKER_SOCKET_URL
            if api_url.startswith("unix://"):
                return True  # para simplicidade, retornar True para unix sockets (pode ser refinado com aiodocker)
                
            timeout = ClientTimeout(total=3.0)
            async with ClientSession(timeout=timeout) as session:
                try:
                    # Verifica se container existe e está rodando
                    container_url = f"{api_url}/containers/{container_id}/json"
                    async with session.get(container_url) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            running = data.get('State', {}).get('Running', False)
                            return running
                        return False  # 404 ou outro erro
                except Exception:
                    return False  # qualquer erro
        except Exception:
            return False
    
    async def _remove_task_permanently(self, task_id: str):
        """
        Remove uma task permanentemente com garantias de segurança.
        - Cancela a task se estiver rodando
        - Remove dos dicionários de gerenciamento
        - Atualiza métricas
        """
        # Cancela task se estiver em execução
        if task_id in self.tasks and not self.tasks[task_id].done():
            try:
                self.tasks[task_id].cancel()
                try:
                    await asyncio.wait_for(self.tasks[task_id], timeout=1.0)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            except Exception as e:
                self.logger.error("Erro ao cancelar tarefa", task_id=task_id, error=str(e))
        
        # Remove das estruturas de controle
        if task_id in self.task_info:
            del self.task_info[task_id]
        if task_id in self.health_status:
            del self.health_status[task_id]
        if task_id in self.tasks:
            del self.tasks[task_id]
        
        # Atualiza métricas
        try:
            metrics.TASK_HEALTH.remove(task_id)
        except Exception:
            pass
        
        self.logger.info("Tarefa removida permanentemente", task_id=task_id)
        return True
    
    async def _orphaned_task_cleanup_loop(self):
        """
        Loop periódico para limpeza de tasks órfãs.
        - Detecta tasks de containers que não existem mais
        - Remove com verificações de segurança
        """
        self.logger.info("Iniciando loop de limpeza de tasks órfãs")
        
        while not self.stop_event.is_set():
            try:
                await asyncio.sleep(ORPHANED_TASK_CLEANUP_INTERVAL)
                
                # Obtém containers ativos do Docker
                from aiohttp import ClientSession, ClientTimeout
                from .config import DOCKER_SOCKET_URL
                
                active_container_ids = set()
                try:
                    timeout = ClientTimeout(total=5.0)
                    async with ClientSession(timeout=timeout) as session:
                        # Adapta URL para funcionar com unix sockets
                        api_url = DOCKER_SOCKET_URL
                        if api_url.startswith("unix://"):
                            self.logger.debug("Pulando limpeza automática para unix socket")
                            continue
                            
                        async with session.get(f"{api_url}/containers/json?all=true") as resp:
                            if resp.status == 200:
                                containers = await resp.json()
                                active_container_ids = {c['Id'][:12] for c in containers}
                            else:
                                self.logger.warning("Falha ao obter lista de containers", status=resp.status)
                                continue
                except Exception as e:
                    self.logger.error("Erro ao conectar com Docker API", error=str(e))
                    continue
                
                # Procura por tasks de container órfãs
                for task_id in list(self.health_status.keys()):
                    if not task_id.startswith("container_"):
                        continue
                        
                    container_id = task_id.replace("container_", "")
                    
                    # Verifica se container existe na lista
                    if container_id in active_container_ids:
                        continue
                    
                    # Verificação extra via API individual
                    container_active = await self._is_container_active(container_id)
                    if container_active:
                        continue
                    
                    # Container confirmadamente não existe, remove task
                    self.logger.info(
                        "Detectada task órfã de container inexistente",
                        task_id=task_id,
                        container_id=container_id
                    )
                    await self._remove_task_permanently(task_id)
                    
            except asyncio.CancelledError:
                self.logger.info("Loop de limpeza de órfãos cancelado")
                break
            except Exception as e:
                self.logger.error(
                    "Erro no loop de limpeza de órfãos",
                    error=str(e),
                    traceback=traceback.format_exc()
                )

# Instância global para uso compartilhado
task_manager = TaskManager()