"""
Módulo de utilitários compartilhados do projeto SQS.

Centraliza criação do cliente boto3 e configuração de logging,
seguindo o princípio DRY.
"""

import logging
import sys
from typing import Any

import boto3
import botocore.exceptions

from src.config import carregar_configuracao


def get_sqs_client(regiao: str | None = None) -> Any:
    """Cria e retorna um cliente boto3 para o Amazon SQS.

    O boto3 busca credenciais automaticamente na ordem:
    1. Variáveis de ambiente (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    2. Arquivo ~/.aws/credentials (configurado via `aws configure`)
    3. IAM role (em instâncias EC2 ou Lambda)

    Args:
        regiao: Região AWS. Se None, usa o valor de AWS_REGION no .env.

    Returns:
        Cliente boto3 do SQS pronto para uso.

    Raises:
        SystemExit: Se as credenciais AWS não estiverem configuradas.
    """
    if regiao is None:
        cfg = carregar_configuracao()
        regiao = cfg.regiao

    try:
        cliente = boto3.client("sqs", region_name=regiao)
        return cliente
    except botocore.exceptions.NoCredentialsError:
        logger = logging.getLogger(__name__)
        logger.error(
            "Credenciais AWS não encontradas. "
            "Execute 'aws configure' para configurar Access Key e Secret."
        )
        sys.exit(1)
    except botocore.exceptions.PartialCredentialsError:
        logger = logging.getLogger(__name__)
        logger.error(
            "Credenciais AWS incompletas. "
            "Verifique se AWS_ACCESS_KEY_ID e AWS_SECRET_ACCESS_KEY estão corretos."
        )
        sys.exit(1)


def configurar_logger(nome: str) -> logging.Logger:
    """Configura e retorna um Logger com formato legível.

    Usa o módulo logging em vez de print() para permitir controle de
    nível de log, redirecionamento para arquivos e integração com ferramentas.

    Args:
        nome: Nome do logger, geralmente __name__ do módulo chamador.

    Returns:
        Logger configurado com handler de console e formato com timestamp.
    """
    logger = logging.getLogger(nome)

    # Evita adicionar múltiplos handlers se o logger já foi configurado
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)

    # Formato: timestamp | nível | módulo | mensagem
    formato = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formato)
    logger.addHandler(handler)

    return logger


def formatar_atributos_mensagem(atributos: dict[str, str]) -> dict[str, dict[str, str]]:
    """Converte um dicionário simples em atributos de mensagem SQS.

    O SQS exige que atributos sigam o formato:
    {"chave": {"StringValue": "valor", "DataType": "String"}}

    Args:
        atributos: Dicionário com pares chave-valor simples.

    Returns:
        Dicionário no formato esperado pela API do SQS.

    Example:
        >>> formatar_atributos_mensagem({"origem": "sistema-a"})
        {"origem": {"StringValue": "sistema-a", "DataType": "String"}}
    """
    return {
        chave: {"StringValue": str(valor), "DataType": "String"}
        for chave, valor in atributos.items()
    }
