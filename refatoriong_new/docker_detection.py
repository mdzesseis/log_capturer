"""
docker_detection.py - Detecção automática do Docker Socket
"""

import os
import socket
import aiohttp
from pathlib import Path
from typing import Optional
from .robustness import StructuredLogger

class DockerSocketDetector:
    """Detecta automaticamente o socket Docker correto"""
    
    def __init__(self):
        self.logger = StructuredLogger("docker_detector")
        
    def detect_socket_url(self) -> str:
        """
        Detecta o socket Docker correto seguindo a ordem de prioridade:
        1. Variável de ambiente DOCKER_HOST
        2. Variável de ambiente DOCKER_SOCKET_URL  
        3. Socket Unix padrão (/var/run/docker.sock)
        4. Docker Desktop (Windows/Mac)
        5. TCP local padrão
        """
        
        # 1. DOCKER_HOST (padrão Docker)
        docker_host = os.getenv("DOCKER_HOST")
        if docker_host:
            self.logger.info("Docker socket detectado via DOCKER_HOST", url=docker_host)
            return self._normalize_docker_url(docker_host)
        
        # 2. DOCKER_SOCKET_URL (nossa configuração)
        socket_url = os.getenv("DOCKER_SOCKET_URL")
        if socket_url:
            self.logger.info("Docker socket configurado via DOCKER_SOCKET_URL", url=socket_url)
            return socket_url
            
        # 3. Socket Unix padrão
        unix_socket = "/var/run/docker.sock"
        if Path(unix_socket).exists():
            # Para aiodocker, mantém formato unix://
            self.logger.info("Docker socket Unix detectado", path=unix_socket)
            return f"unix://{unix_socket}"
            
        # 4. Docker Desktop (Windows/Mac)
        desktop_paths = [
            "~/.docker/desktop/docker.sock",
            "/var/run/docker.sock",
            "//./pipe/docker_engine",  # Windows named pipe
        ]
        
        for path in desktop_paths:
            expanded = Path(path).expanduser()
            if expanded.exists():
                self.logger.info("Docker Desktop socket detectado", path=str(expanded))
                return f"unix://{expanded}"
        
        # 5. TCP local padrão
        tcp_candidates = [
            "http://localhost:2375",   # Docker API sem TLS
            "http://localhost:2376",   # Docker API com TLS
            "http://127.0.0.1:2375",
            "tcp://localhost:2375",
        ]
        
        for url in tcp_candidates:
            if self._test_tcp_connection(url):
                self.logger.info("Docker TCP detectado", url=url)
                return self._normalize_docker_url(url)
        
        # 6. Fallback padrão
        fallback = "http://localhost:2375"
        self.logger.warning("Nenhum socket Docker detectado, usando fallback", fallback=fallback)
        return fallback
    
    def _normalize_docker_url(self, url: str) -> str:
        """Normaliza URL para formato HTTP para uso com aiohttp"""
        if url.startswith("tcp://"):
            return url.replace("tcp://", "http://")
        if url.startswith("unix://"):
            # Para verificações HTTP via socket, usar proxy local ou manter unix://
            return url
        return url
    
    def _test_tcp_connection(self, url: str) -> bool:
        """Testa se consegue conectar no endpoint TCP"""
        try:
            # Extrai host e porta da URL
            if url.startswith(("http://", "https://", "tcp://")):
                parts = url.split("://")[1].split(":")
                host = parts[0]
                port = int(parts[1]) if len(parts) > 1 else 2375
            else:
                return False
                
            # Testa conexão TCP
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2)
            result = sock.connect_ex((host, port))
            sock.close()
            return result == 0
            
        except Exception:
            return False

    async def test_docker_api(self, url: str) -> bool:
        """Testa se a API Docker responde corretamente"""
        try:
            if url.startswith("unix://"):
                # Para Unix socket, precisa usar aiodocker diretamente
                import aiodocker
                docker = aiodocker.Docker(url=url)
                await docker.system.info()
                await docker.close()
                return True
            else:
                # Para HTTP, pode usar aiohttp
                async with aiohttp.ClientSession() as session:
                    async with session.get(f"{url}/version", timeout=5) as resp:
                        return resp.status == 200
        except Exception as e:
            self.logger.debug("Teste de API Docker falhou", url=url, error=str(e))
            return False

# Instância global
docker_detector = DockerSocketDetector()
