import time
from enum import Enum
from dataclasses import dataclass
from .config import CIRCUIT_BREAKER_ENABLED, CIRCUIT_BREAKER_FAILURE_THRESHOLD, CIRCUIT_BREAKER_RECOVERY_TIMEOUT

class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = CIRCUIT_BREAKER_FAILURE_THRESHOLD
    recovery_timeout: int = CIRCUIT_BREAKER_RECOVERY_TIMEOUT
    enabled: bool = CIRCUIT_BREAKER_ENABLED

class CircuitBreaker:
    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self.half_open_calls = 0
        self.success_count = 0
    async def call(self, func, *args, **kwargs):
        if not self.config.enabled:
            return await func(*args, **kwargs)
        if self.state == CircuitBreakerState.OPEN:
            if time.time() - self.last_failure_time > self.config.recovery_timeout:
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
            else:
                raise Exception(f"Circuit breaker {self.name} is OPEN")
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
        except Exception:
            self._on_failure()
            raise
    def _on_success(self):
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= 3:
                self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
    def _on_failure(self):
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.state == CircuitBreakerState.HALF_OPEN or self.failure_count >= self.config.failure_threshold:
            self.state = CircuitBreakerState.OPEN
