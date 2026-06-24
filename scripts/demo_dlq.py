"""
Script de demonstração da Dead-Letter Queue (DLQ).

Executa ao vivo durante a apresentação para mostrar o comportamento da DLQ:

    1. Uma mensagem é enviada para a fila principal (Standard).
    2. A mensagem é recebida repetidamente sem ser deletada.
       (simula um consumidor com falha de processamento)
    3. Após MAX_RECEIVE_COUNT tentativas, o SQS move a mensagem para a DLQ.
    4. O script confirma que a mensagem chegou na DLQ.

Por que a DLQ existe?
    Sem DLQ, uma mensagem que sempre falha ficaria em loop infinitamente
    na fila principal, bloqueando outros consumidores e gerando
    custo de processamento infinito.

Uso:
    python scripts/demo_dlq.py

Pré-requisito: .env preenchido com as URLs das filas.
"""

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import botocore.exceptions

from src.config import carregar_configuracao_completa
from src.utils import get_sqs_client, configurar_logger
from src.producer import enviar_mensagem

logger = configurar_logger(__name__)


def verificar_dlq(cliente, url_dlq: str, max_tentativas: int = 10) -> dict | None:
    """Aguarda e verifica se uma mensagem chegou na DLQ.

    Faz polling na DLQ por até max_tentativas vezes, com intervalo de 2s.
    O SQS pode levar alguns segundos para move a mensagem para a DLQ.

    Args:
        cliente: Cliente boto3 do SQS.
        url_dlq: URL da Dead-Letter Queue.
        max_tentativas: Número máximo de verificações.

    Returns:
        A primeira mensagem encontrada na DLQ, ou None se não encontrada.
    """
    logger.info(f"Verificando DLQ (até {max_tentativas} tentativas)...")

    for tentativa in range(1, max_tentativas + 1):
        logger.info(f"  Tentativa {tentativa}/{max_tentativas}...")
        resposta = cliente.receive_message(
            QueueUrl=url_dlq,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=2,  # short polling para demo mais ágil
            AttributeNames=["All"],
        )
        mensagens = resposta.get("Messages", [])
        if mensagens:
            msg = mensagens[0]
            # Deleta da DLQ para não acumular entre demonstrações
            cliente.delete_message(
                QueueUrl=url_dlq,
                ReceiptHandle=msg["ReceiptHandle"],
            )
            return msg
        time.sleep(2)

    return None


def simular_falhas(
    cliente,
    url_fila: str,
    receipt_handle: str,
    max_recebimentos: int,
    visibility_timeout: int = 1,
) -> None:
    """Recebe a mesma mensagem repetidamente sem deletá-la.

    A cada recebimento sem delete, o contador ApproximateReceiveCount aumenta.
    Quando ultrapassa maxReceiveCount da RedrivePolicy, o SQS move a mensagem
    para a DLQ automaticamente.

    Args:
        cliente: Cliente boto3 do SQS.
        url_fila: URL da fila onde a mensagem está.
        receipt_handle: ReceiptHandle da primeira recepção.
        max_recebimentos: Quantas falhas são necessárias para acionar a DLQ.
        visibility_timeout: Timeout de visibilidade em segundos.
    """
    # Muda o VisibilityTimeout para 1 segundo para acelerar a demonstração.
    # Normalmente seria 30s, mas aqui nós usamos 1s para não precisar esperar muito tempo.
    cliente.change_message_visibility(
        QueueUrl=url_fila,
        ReceiptHandle=receipt_handle,
        VisibilityTimeout=visibility_timeout,
    )
    logger.info(f"VisibilityTimeout ajustado para {visibility_timeout}s para acelerar a demonstração.")

    for tentativa in range(2, max_recebimentos + 2):
        logger.info(f"Aguardando mensagem ficar visível novamente...")
        time.sleep(visibility_timeout + 1)

        resposta = cliente.receive_message(
            QueueUrl=url_fila,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=3,
            AttributeNames=["All"],
        )
        mensagens = resposta.get("Messages", [])

        if not mensagens:
            logger.info(
                f"Mensagem não recebida na tentativa {tentativa} — "
                "provavelmente já foi para a DLQ."
            )
            break

        msg = mensagens[0]
        contagem = msg.get("Attributes", {}).get("ApproximateReceiveCount", "?")
        logger.warning(
            f"FALHA SIMULADA #{tentativa} | "
            f"ApproximateReceiveCount={contagem} | "
            f"Não deletando — mensagem voltará à fila."
        )
        # Reduz o timeout novamente para acelerar a próxima iteração
        cliente.change_message_visibility(
            QueueUrl=url_fila,
            ReceiptHandle=msg["ReceiptHandle"],
            VisibilityTimeout=visibility_timeout,
        )


def main() -> None:
    """Executa a demonstração completa da Dead-Letter Queue."""
    cfg = carregar_configuracao_completa()
    cliente = get_sqs_client(cfg.regiao)

    print("\n" + "=" * 60)
    print("DEMONSTRAÇÃO: Dead-Letter Queue (DLQ)")
    print("=" * 60)

    # PASSO 1: Enviar mensagem para a fila principal
    print("\n[PASSO 1] Enviando mensagem para a fila Standard...")
    resposta_envio = enviar_mensagem(
        queue_url=cfg.url_fila_standard,
        corpo="Mensagem que vai falhar para demonstração da DLQ",
        atributos={"tipo": "demo-dlq", "origem": "demo_dlq.py"},
    )
    logger.info(f"Mensagem enviada com ID: {resposta_envio['MessageId']}")

    # PASSO 2: Receber a mensagem pela primeira vez
    print(f"\n[PASSO 2] Recebendo a mensagem (vamos falhar {cfg.max_recebimentos}x)...")
    time.sleep(1)  # pequena pausa para garantir que a mensagem esteja disponível

    resposta_recv = cliente.receive_message(
        QueueUrl=cfg.url_fila_standard,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5,
        AttributeNames=["All"],
    )
    mensagens = resposta_recv.get("Messages", [])
    if not mensagens:
        logger.error("Nenhuma mensagem recebida. Verifique se a fila Standard está correta.")
        sys.exit(1)

    msg = mensagens[0]
    contagem = msg.get("Attributes", {}).get("ApproximateReceiveCount", "?")
    logger.warning(
        f"FALHA SIMULADA #1 | ApproximateReceiveCount={contagem} | "
        f"Corpo: '{msg['Body']}' | Não deletando."
    )

    # PASSO 3: Simular falhas repetidas
    print(f"\n[PASSO 3] Simulando mais {cfg.max_recebimentos - 1} falha(s)...")
    simular_falhas(
        cliente=cliente,
        url_fila=cfg.url_fila_standard,
        receipt_handle=msg["ReceiptHandle"],
        max_recebimentos=cfg.max_recebimentos,
        visibility_timeout=1,
    )

    # PASSO 4: Verificar se chegou na DLQ
    print("\n[PASSO 4] Verificando se a mensagem chegou na DLQ...")
    time.sleep(3)

    msg_dlq = verificar_dlq(cliente, cfg.url_dlq)

    print("\n" + "=" * 60)
    if msg_dlq:
        print("RESULTADO: Mensagem encontrada na DLQ!")
        print(f"  ID da mensagem: {msg_dlq['MessageId']}")
        print(f"  Corpo: '{msg_dlq['Body']}'")
        print(
            "  Conclusão: após exceder maxReceiveCount, "
            "o SQS moveu automaticamente para a DLQ."
        )
    else:
        print("RESULTADO: Mensagem ainda não chegou na DLQ.")
        print("  Tente aguardar alguns segundos e checar o console AWS.")
    print("=" * 60)


if __name__ == "__main__":
    main()
