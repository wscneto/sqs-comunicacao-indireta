"""
Script de criação de filas SQS (infraestrutura como código).

Em vez de criar filas manualmente no console AWS, este script cria toda a
infraestrutura de forma reprodutível e versionada, permitindo que qualquer
membro do grupo recrie o ambiente do zero rodando apenas este arquivo.

Filas criadas:
    - demo-standard   : Fila Standard (at-least-once, ordem não garantida)
    - demo-fifo.fifo  : Fila FIFO (exactly-once, ordem garantida por grupo)
    - demo-dlq        : Dead-Letter Queue Standard (DLQ da fila Standard)
    - demo-dlq.fifo   : Dead-Letter Queue FIFO (DLQ da fila FIFO)

Observação importante do SQS: a DLQ precisa ser do mesmo tipo da fila de
origem. Por isso a fila Standard usa uma DLQ Standard e a fila FIFO usa uma
DLQ FIFO. Não é possível compartilhar a mesma DLQ entre os dois tipos.

Uso:
    python scripts/create_queues.py

Pré-requisito: AWS configurado via `aws configure`.
"""

import json
import sys
import os

# Permite importar src.* sem instalar o pacote
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import botocore.exceptions

from src.config import carregar_configuracao
from src.utils import get_sqs_client, configurar_logger

logger = configurar_logger(__name__)


def criar_dlq(cliente, cfg, fifo: bool = False) -> str:
    """Cria uma Dead-Letter Queue (DLQ) e retorna sua URL.

    A DLQ precisa existir antes das filas principais, pois a RedrivePolicy
    das filas principais referencia o ARN (Amazon Resource Name) da DLQ.

    Como o SQS exige que a DLQ seja do mesmo tipo da fila de origem, esta
    função cria uma DLQ Standard (fifo=False) ou uma DLQ FIFO (fifo=True).

    Args:
        cliente: Cliente boto3 do SQS.
        cfg: Configuração do projeto.
        fifo: Se True, cria uma DLQ do tipo FIFO (nome termina em '.fifo').

    Returns:
        URL da DLQ criada (ou já existente).
    """
    nome = "demo-dlq.fifo" if fifo else "demo-dlq"
    logger.info(f"Criando Dead-Letter Queue: {nome}")

    atributos = {
        # Quanto tempo a mensagem fica disponível na fila (segundos).
        # 14 dias é o máximo permitido pelo SQS.
        "MessageRetentionPeriod": "1209600",  # 14 dias
        # Tempo que a mensagem fica invisível após ser recebida.
        "VisibilityTimeout": "30",
    }
    # Uma DLQ FIFO também precisa do atributo FifoQueue=true.
    if fifo:
        atributos["FifoQueue"] = "true"

    try:
        resposta = cliente.create_queue(
            QueueName=nome,
            Attributes=atributos,
        )
        url = resposta["QueueUrl"]
        logger.info(f"DLQ criada com sucesso: {url}")
        return url
    except botocore.exceptions.ClientError as e:
        codigo = e.response["Error"]["Code"]
        # QueueAlreadyExists: a fila já existe com os mesmos atributos, ok.
        if codigo == "QueueAlreadyExists":
            logger.info(f"DLQ já existe. Obtendo URL existente...")
            resposta = cliente.get_queue_url(QueueName=nome)
            return resposta["QueueUrl"]
        raise


def obter_arn_fila(cliente, url_fila: str) -> str:
    """Obtém o ARN (Amazon Resource Name) de uma fila a partir de sua URL.

    O ARN é necessário para configurar a RedrivePolicy, pois ele identifica
    univocamente a DLQ dentro da AWS.

    Args:
        cliente: Cliente boto3 do SQS.
        url_fila: URL da fila.

    Returns:
        ARN da fila no formato arn:aws:sqs:regiao:conta:nome.
    """
    resposta = cliente.get_queue_attributes(
        QueueUrl=url_fila,
        AttributeNames=["QueueArn"],
    )
    return resposta["Attributes"]["QueueArn"]


def criar_fila_standard(cliente, cfg, arn_dlq: str) -> str:
    """Cria a fila Standard com RedrivePolicy apontando para a DLQ.

    Fila Standard: entrega at-least-once (pode entregar duplicatas) e
    não garante ordem das mensagens. Alto throughput.

    Args:
        cliente: Cliente boto3 do SQS.
        cfg: Configuração do projeto.
        arn_dlq: ARN da DLQ para configurar o redirecionamento.

    Returns:
        URL da fila Standard criada.
    """
    nome = "demo-standard"
    logger.info(f"Criando fila Standard: {nome}")

    # RedrivePolicy: após MAX_RECEIVE_COUNT falhas, a mensagem vai para a DLQ.
    # maxReceiveCount=3 significa que na 4ª tentativa falha, a msg vai para a DLQ.
    redrive_policy = json.dumps({
        "maxReceiveCount": str(cfg.max_recebimentos),
        "deadLetterTargetArn": arn_dlq,
    })

    try:
        resposta = cliente.create_queue(
            QueueName=nome,
            Attributes={
                "VisibilityTimeout": "30",
                # MessageRetentionPeriod: quanto tempo SQS mantém a mensagem
                # antes de descartá-la automaticamente (4 dias aqui).
                "MessageRetentionPeriod": "345600",  # 4 dias
                # Long polling: reduz chamadas vazias, recebe mensagem mais rápido.
                "ReceiveMessageWaitTimeSeconds": str(cfg.tempo_espera_segundos),
                "RedrivePolicy": redrive_policy,
            },
        )
        url = resposta["QueueUrl"]
        logger.info(f"Fila Standard criada: {url}")
        return url
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "QueueAlreadyExists":
            logger.info(f"Fila Standard já existe. Obtendo URL...")
            resposta = cliente.get_queue_url(QueueName=nome)
            return resposta["QueueUrl"]
        raise


def criar_fila_fifo(cliente, cfg, arn_dlq: str) -> str:
    """Cria a fila FIFO com deduplicação baseada em conteúdo.

    Fila FIFO: garante ordem de entrega (First In, First Out) e exatamente
    uma entrega (exactly-once) dentro de um mesmo MessageGroupId.
    O nome deve terminar em '.fifo'.

    Args:
        cliente: Cliente boto3 do SQS.
        cfg: Configuração do projeto.
        arn_dlq: ARN da DLQ para configurar o redirecionamento.

    Returns:
        URL da fila FIFO criada.
    """
    nome = "demo-fifo.fifo"
    logger.info(f"Criando fila FIFO: {nome}")

    redrive_policy = json.dumps({
        "maxReceiveCount": str(cfg.max_recebimentos),
        "deadLetterTargetArn": arn_dlq,
    })

    try:
        resposta = cliente.create_queue(
            QueueName=nome,
            Attributes={
                # FifoQueue=true: obrigatório para filas FIFO.
                "FifoQueue": "true",
                # ContentBasedDeduplication: o SQS calcula o ID de deduplicação
                # automaticamente com base no hash SHA-256 do corpo da mensagem.
                # Isso evita duplicatas sem precisar gerar IDs manualmente.
                "ContentBasedDeduplication": "true",
                "VisibilityTimeout": "30",
                "MessageRetentionPeriod": "345600",  # 4 dias
                "ReceiveMessageWaitTimeSeconds": str(cfg.tempo_espera_segundos),
                "RedrivePolicy": redrive_policy,
            },
        )
        url = resposta["QueueUrl"]
        logger.info(f"Fila FIFO criada: {url}")
        return url
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "QueueAlreadyExists":
            logger.info(f"Fila FIFO já existe. Obtendo URL...")
            resposta = cliente.get_queue_url(QueueName=nome)
            return resposta["QueueUrl"]
        raise


def main() -> None:
    """Ponto de entrada principal do script de criação de filas."""
    logger.info("=== Iniciando criação de filas SQS ===")

    cfg = carregar_configuracao()
    cliente = get_sqs_client(cfg.regiao)

    # As DLQs devem existir antes das filas principais.
    # Cada fila usa uma DLQ do mesmo tipo (exigência do SQS).
    url_dlq = criar_dlq(cliente, cfg, fifo=False)
    arn_dlq = obter_arn_fila(cliente, url_dlq)
    logger.info(f"ARN da DLQ Standard: {arn_dlq}")

    url_dlq_fifo = criar_dlq(cliente, cfg, fifo=True)
    arn_dlq_fifo = obter_arn_fila(cliente, url_dlq_fifo)
    logger.info(f"ARN da DLQ FIFO: {arn_dlq_fifo}")

    url_standard = criar_fila_standard(cliente, cfg, arn_dlq)
    url_fifo = criar_fila_fifo(cliente, cfg, arn_dlq_fifo)

    print("\n" + "=" * 60)
    print("FILAS CRIADAS COM SUCESSO!")
    print("Copie os valores abaixo para o seu arquivo .env:")
    print("=" * 60)
    print(f"STANDARD_QUEUE_URL={url_standard}")
    print(f"FIFO_QUEUE_URL={url_fifo}")
    print(f"DLQ_URL={url_dlq}")
    print(f"DLQ_FIFO_URL={url_dlq_fifo}")
    print("=" * 60)


if __name__ == "__main__":
    main()
