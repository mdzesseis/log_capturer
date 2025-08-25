"""
task_manager.py - Gerenciamento assíncrono de tarefas do agente.
- Isolamento por grupos, prioridades, health check, restart automático.
- Fluxo robusto para garantir continuidade e observabilidade.
"""

import time
import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Set, Callable
from .config import TASK_RESTART_ENABLED, TASK_MAX_RESTART_COUNT, TASK_RESTART_BACKOFF_BASE, TASK_HEALTH_CHECK_INTERVAL
from .config import TASK_MAX_RESTARTS_PER_HOUR, TASK_CIRCUIT_BREAKER_DURATION
import random
from .metrics import metrics
from .robustness import StructuredLogger  # + log estruturado

@dataclass
class TaskHealth:
    name: str
    status: str = "healthy"
    last_heartbeat: float = field(default_factory=time.time)
    restart_count: int = 0
    last_error: Optional[str] = None
    consecutive_failures: int = 0
    last_restart_time: float = 0
    # NOVO: controle de janela e circuit-open por tarefa
    restart_window_start: float = 0.0
    restarts_in_window: int = 0
    circuit_open_until: float = 0.0

    def _calc_backoff_delay(self) -> float:
        if self.restart_count <= 0:
            return 0.0
        base = min(TASK_RESTART_BACKOFF_BASE ** self.restart_count, 3600.0)
        return base * random.uniform(0.8, 1.2)

    def should_restart(self, current_time: Optional[float] = None) -> bool:
        current_time = current_time or time.time()
        # Circuit aberto
        if current_time < self.circuit_open_until:
            return False
        # Janela de 1h - verificar limite ANTES de resetar
        window = 3600.0
        window_expired = current_time - self.restart_window_start > window
        if window_expired:
            if self.restarts_in_window < TASK_MAX_RESTARTS_PER_HOUR:
                self.restart_window_start = current_time
                self.restarts_in_window = 0
            else:
                self.circuit_open_until = current_time + TASK_CIRCUIT_BREAKER_DURATION
                return False
        if self.restarts_in_window >= TASK_MAX_RESTARTS_PER_HOUR:
            self.circuit_open_until = current_time + TASK_CIRCUIT_BREAKER_DURATION
            return False
        # Backoff
        if self.last_restart_time > 0:
            if current_time - self.last_restart_time < self._calc_backoff_delay():
                return False
        # Limite global
        if self.restart_count >= TASK_MAX_RESTART_COUNT:
            return False
        return True

    def on_restart(self, current_time: Optional[float] = None):
        current_time = current_time or time.time()
        self.restart_count += 1
        self.restarts_in_window += 1
        self.last_restart_time = current_time

class TaskPriority(Enum):
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3

class TaskManager:
    def __init__(self):
        self.tasks: Dict[str, asyncio.Task] = {}
        self.task_factories: Dict[str, Callable] = {}
        self.health_status: Dict[str, TaskHealth] = {}
        self.health_check_task: Optional[asyncio.Task] = None
        # NOVO: referência do dispatcher para cancelamento no shutdown
        self.dispatcher_task: Optional[asyncio.Task] = None
        self.isolated_groups: Dict[str, Set[str]] = {}
        self.task_priorities: Dict[str, TaskPriority] = {}
        self.priority_queues: Dict[TaskPriority, asyncio.PriorityQueue] = {
            TaskPriority.CRITICAL: asyncio.PriorityQueue(),
            TaskPriority.HIGH: asyncio.PriorityQueue(),
            TaskPriority.NORMAL: asyncio.PriorityQueue(),
            TaskPriority.LOW: asyncio.PriorityQueue()
        }
        self.logger = StructuredLogger("task_manager")  # + logger
        # Labels extras por tarefa (ex.: container_name para tarefas de container)
        self.task_extra_labels: Dict[str, Dict[str, str]] = {}

    def set_task_labels(self, name: str, labels: Dict[str, str]):
        """Define/atualiza labels extras associadas à tarefa (ex.: container_name)."""
        self.task_extra_labels.setdefault(name, {}).update(labels)

    async def start(self):
        # Logs de inicialização
        self.logger.info("Iniciando TaskManager", restart_enabled=TASK_RESTART_ENABLED)
        if TASK_RESTART_ENABLED:
            self.dispatcher_task = asyncio.create_task(self._task_dispatcher())
            self.health_check_task = asyncio.create_task(self._health_check_loop())
            self.logger.info("TaskManager iniciado com dispatcher e health_check")
        else:
            self.logger.info("Restart automático desativado")

    async def stop(self):
        self.logger.info("Parando TaskManager", tasks_count=len(self.tasks))
        # NOVO: cancelar dispatcher primeiro para evitar enfileiramento durante o shutdown
        if self.dispatcher_task:
            self.dispatcher_task.cancel()
            try:
                await self.dispatcher_task
            except asyncio.CancelledError:
                pass
            finally:
                self.dispatcher_task = None
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
            finally:
                self.health_check_task = None
        for group_name, task_names in self.isolated_groups.items():
            group_tasks = [self.tasks[name] for name in task_names if name in self.tasks]
            for task in group_tasks: task.cancel()
            await asyncio.gather(*group_tasks, return_exceptions=True)
        remaining = [t for t in self.tasks.values() if not t.done()]
        if remaining:
            self.logger.debug("Aguardando tarefas restantes concluírem", remaining=len(remaining))
            await asyncio.gather(*remaining, return_exceptions=True)
        self.logger.info("TaskManager parado")

    def register_task(self, name: str, factory: Callable, group: str = "default", priority: TaskPriority = TaskPriority.NORMAL, *args, **kwargs):
        if name in self.tasks and not self.tasks[name].done():
            self.logger.warning("Tentativa de registrar tarefa já em execução ignorada", task_name=name)
            return
        self.task_factories[name] = lambda: factory(*args, **kwargs)
        self.health_status[name] = TaskHealth(name)
        self.task_priorities[name] = priority
        self.isolated_groups.setdefault(group, set()).add(name)
        self.logger.info("Tarefa registrada", task_name=name, group=group, priority=priority.name)

    async def start_task(self, name: str):
        if name in self.tasks and not self.tasks[name].done():
            self.logger.debug("Solicitado start de tarefa já em execução", task_name=name)
            return
        priority = self.task_priorities.get(name, TaskPriority.NORMAL)
        await self.priority_queues[priority].put((time.time(), name))
        self.logger.debug("Tarefa enfileirada para start", task_name=name, priority=priority.name, queue_size=self.priority_queues[priority].qsize())

    async def _task_dispatcher(self):
        self.logger.info("Dispatcher de tarefas iniciado")
        while True:
            try:
                for priority in [TaskPriority.CRITICAL, TaskPriority.HIGH, TaskPriority.NORMAL, TaskPriority.LOW]:
                    if not self.priority_queues[priority].empty():
                        _, name = await self.priority_queues[priority].get()
                        self.logger.debug("Despachando tarefa", task_name=name, priority=priority.name)
                        await self._execute_task(name)
                        break
                else:
                    await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                self.logger.info("Dispatcher cancelado")
                break
            except Exception as e:
                self.logger.error("Erro no dispatcher", error=str(e))
                await asyncio.sleep(1)

    async def _execute_task(self, name: str):
        if name in self.tasks and not self.tasks[name].done():
            self.logger.debug("Ignorando execução; já em andamento", task_name=name)
            return
        try:
            coro = self.task_factories[name]()
        except Exception as e:
            self.logger.error("Falha ao criar coroutine da tarefa", task_name=name, error=str(e))
            return
        task = asyncio.create_task(self._isolated_task_wrapper(name, coro))
        self.tasks[name] = task
        health = self.health_status[name]
        health.last_restart_time = time.time()
        metrics.TASK_HEALTH.labels(task_name=name).set(1)
        self.logger.info("Tarefa iniciada", task_name=name, restart_count=health.restart_count)

    async def _isolated_task_wrapper(self, name: str, coro):
        try:
            await coro
            self.logger.debug("Tarefa finalizada sem exceção", task_name=name)
        except asyncio.CancelledError:
            self.logger.info("Tarefa cancelada", task_name=name)
            raise
        except Exception as e:
            health = self.health_status[name]
            health.status = "failed"
            health.last_error = str(e)
            health.consecutive_failures += 1
            metrics.TASK_HEALTH.labels(task_name=name).set(0)
            self.logger.error("Tarefa falhou", task_name=name, consecutive_failures=health.consecutive_failures, error=str(e))

    async def heartbeat(self, name: str):
        if name in self.health_status:
            health = self.health_status[name]
            prev_status = health.status
            health.last_heartbeat = time.time()
            if health.status != "healthy":
                health.status = "healthy"
                health.consecutive_failures = 0
                metrics.TASK_HEALTH.labels(task_name=name).set(1)
                self.logger.info("Tarefa recuperou saúde", task_name=name, previous_status=prev_status)

    async def _health_check_loop(self):
        self.logger.info("Health check loop iniciado", interval=TASK_HEALTH_CHECK_INTERVAL)
        while True:
            try:
                now = time.time()
                for name, health in sorted(self.health_status.items(), key=lambda x: self.task_priorities.get(x[0], TaskPriority.NORMAL).value):
                    await self._check_individual_task_health(name, health, now)
                await asyncio.sleep(TASK_HEALTH_CHECK_INTERVAL)
            except asyncio.CancelledError:
                self.logger.info("Health check cancelado")
                break
            except Exception as e:
                self.logger.error("Erro crítico no health check", error=str(e))
                await asyncio.sleep(5)

    async def _check_individual_task_health(self, name: str, health: TaskHealth, current_time: float):
        heartbeat_timeout = TASK_HEALTH_CHECK_INTERVAL * 3
        if current_time - health.last_heartbeat > heartbeat_timeout:
            if name in self.tasks and not self.tasks[name].done():
                self.logger.warning("Timeout de heartbeat; cancelando tarefa", task_name=name, last_heartbeat=health.last_heartbeat, now=current_time)
                self.tasks[name].cancel()
                health.status = "timeout"
                metrics.TASK_HEALTH.labels(task_name=name).set(0)

        if name in self.tasks and self.tasks[name].done():
            should_consider_restart = (not self.tasks[name].cancelled()) or (health.status == "timeout")

            if should_consider_restart:
                # Política unificada com rate limit/backoff
                if health.should_restart(current_time):
                    health.on_restart(current_time)
                    self.logger.warning("Reiniciando tarefa", task_name=name, restart_count=health.restart_count)
                    await self.start_task(name)
                    container_name = self.task_extra_labels.get(name, {}).get("container_name", "n/a")
                    metrics.TASK_RESTARTS.labels(task_name=name, container_name=container_name).inc()
                else:
                    # Marca como em "circuit_open" para visibilidade
                    if health.status != "circuit_open":
                        self.logger.error("Tarefa em circuit breaker ou excedeu limites", task_name=name, restart_count=health.restart_count, last_error=health.last_error, circuit_open_until=health.circuit_open_until)
                        health.status = "circuit_open"
                        metrics.TASK_HEALTH.labels(task_name=name).set(0)

    def get_task_status(self):
        status = {}
        for name, health in self.health_status.items():
            running = name in self.tasks and not self.tasks[name].done()
            status[name] = {
                "status": health.status,
                "running": running,
                "restart_count": health.restart_count,
                "consecutive_failures": health.consecutive_failures,
                "last_error": health.last_error,
                "last_heartbeat": health.last_heartbeat
            }
        return status

task_manager = TaskManager()
