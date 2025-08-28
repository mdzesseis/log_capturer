#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio

# NOTA: O código foi refatorado para o pacote "refatoriong_new".
# Este script mantém compatibilidade retroativa delegando para o novo entrypoint.
# Para desenvolvimento, use diretamente: python -m refatoriong_new

async def _run():
    """
    Entrypoint de compatibilidade que delega para a implementação refatorada.
    """
    try:
        from refatoriong_new.__main__ import main
        await main()
    except ImportError as e:
        print(f"Erro: Pacote refatoriong_new não encontrado: {e}")
        print("Certifique-se de que o pacote está instalado corretamente.")
        raise
    except Exception as e:
        print(f"Erro na execução: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(_run())