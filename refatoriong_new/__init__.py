# Inicialização do pacote refatorado
# Evitar importações pesadas aqui para não gerar ciclos ou efeitos colaterais no startup.

# Se desejar expor versão/metadata leve, faça aqui sem carregar módulos grandes.
__all__ = []
from .optimization import http_session_pool, optimized_file_executor, unified_cleanup_manager
from .task_manager import task_manager
