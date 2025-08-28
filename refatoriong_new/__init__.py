# Inicialização do pacote refatorado
# Evitar importações pesadas aqui para não gerar ciclos ou efeitos colaterais no startup.

# Se desejar expor versão/metadata leve, faça aqui sem carregar módulos grandes.
__all__ = []

# REMOVIDO: imports que causam dependência circular na inicialização
# Estes serão importados quando necessários pelos módulos específicos
