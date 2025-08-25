import os
from typing import Optional
from .config import SECRETS_MANAGER, VAULT_ADDR, VAULT_TOKEN, AWS_REGION

class SecretManager:
    def __init__(self):
        self.backend = SECRETS_MANAGER
        self.client = None
        if self.backend == 'vault':
            try:
                import hvac
                self.client = hvac.Client(url=VAULT_ADDR, token=VAULT_TOKEN)
            except ImportError:
                self.client = None
        elif self.backend == 'aws':
            try:
                import boto3
                self.client = boto3.client('secretsmanager', region_name=AWS_REGION)
            except ImportError:
                self.client = None
    def get_secret(self, secret_key: str) -> Optional[str]:
        env_value = os.getenv(secret_key)
        if env_value:
            return env_value
        if not self.client:
            return None
        try:
            if self.backend == 'vault':
                response = self.client.secrets.kv.v2.read_secret_version(path=secret_key)
                return response['data']['data'].get('value')
            if self.backend == 'aws':
                response = self.client.get_secret_value(SecretId=secret_key)
                return response.get('SecretString')
        except Exception:
            return None
