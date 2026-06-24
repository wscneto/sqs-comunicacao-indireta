"""
Módulo consumidor de mensagens SQS.

Responsável por receber, processar e deletar mensagens de filas SQS.
Demonstra long polling, o ciclo de vida da mensagem e o papel do
visibility timeout na garantia de entrega.

Uso via linha de comando:
    python -m src.consumer --fila standard
    python -m src.consumer --fila fifo
    python -m src.consumer --fila standard --simular-falha

Encerramento: pressione Ctrl+C.
"""

import argparse
import sys
import os
import time
from typing import Optional

import botocore.exceptions

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import carregar_configuracao_completa
from src.utils import get_sqs_client, configurar_logger

logger = configurar_logger(__name__)

# Máximo de mensagens por chamada receive_message (limite da API SQS)
MAX_MENSAGENS_POR_CHAMADA = 10


def receber_mensagens(
    queue_url: str,
    tempo_espera: int = 10,
    max_mensagens: int = 10,
) -> list[dict]:
    """Recebe mensagens de uma fila SQS usando long polling.

    Long polling: o SQS espera até 'tempo_espera' segundos por mensagens
    antes de retornar uma resposta vazia. Isso reduz o número de chamadas
    à API (e o custo), em comparação ao short polling que retorna imediatamente.

    Após ser recebida, a mensagem fica invisível para outros consumidores
    durante o VisibilityTimeout (padrão: 30s). Se não for deletada nesse
    período, ela reaparece na fila para nova tentativa.

    Args:
        queue_url: URL da fila de origem.
        tempo_espera: Segundos de long polling (0 = short polling, máx: 20).
        max_mensagens: Máximo de mensagens a buscar por chamada (máx: 10).

    Returns:
        Lista de dicionários com as mensagens recebidas.
        Cada mensagem tem: Body, MessageId, ReceiptHandle, Attributes, etc.
    """
    cliente = get_sqs_client()

    try:
        resposta = cliente.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(max_mensagens, MAX_MENSAGENS_POR_CHAMADA),
            # Long polling: aguarda até N segundos por mensagens.
            WaitTimeSeconds=tempo_espera,
            # Atributos da mensagem que queremos receber
            AttributeNames=["All"],
            MessageAttributeNames=["All"],
        )
        mensagens = resposta.get("Messages", [])
        if mensagens:
            logger.info(f"Recebidas {len(mensagens)} mensagem(s) da fila.")
        return mensagens
    except botocore.exceptions.ClientError as e:
        codigo = e.response["Error"]["Code"]
        logger.error(f"Erro ao receber mensagens: [{codigo}] {e.response['Error']['Message']}")
        return []


def processar_mensagem(mensagem: dict, simular_falha: bool = False) -> bool:
    """Simula o processamento de uma mensagem.

    Em um sistema real, aqui estaria a lógica de negócio: gravar no banco,
    chamar uma API, enviar um e-mail, etc.

    Se simular_falha=True, a mensagem NÃO é processada e NÃO é deletada.
    Isso demonstra o comportamento da DLQ: após MAX_RECEIVE_COUNT falhas
    a mensagem é redirecionada automaticamente.

    Args:
        mensagem: Dicionário com os dados da mensagem SQS.
        simular_falha: Se True, simula uma falha de processamento.

    Returns:
        True se o processamento foi bem-sucedido, False em caso de falha.
    """
    msg_id = mensagem.get("MessageId", "desconhecido")
    corpo = mensagem.get("Body", "")
    atributos = mensagem.get("MessageAttributes", {})
    contagem_recebimentos = mensagem.get("Attributes", {}).get("ApproximateReceiveCount", "?")

    logger.info(
        f"Processando mensagem | ID: {msg_id} | "
        f"Tentativa: {contagem_recebimentos} | Corpo: '{corpo}'"
    )

    if atributos:
        for nome, dados in atributos.items():
            logger.info(f"  Atributo '{nome}': {dados.get('StringValue', '')}")

    if simular_falha:
        logger.warning(
            f"FALHA SIMULADA para mensagem {msg_id} — "
            f"não será deletada; voltará à fila após o VisibilityTimeout."
        )
        return False

    logger.info(f"Mensagem {msg_id} processada com sucesso.")
    return True


def deletar_mensagem(queue_url: str, receipt_handle: str) -> None:
    """Remove uma mensagem da fila após processamento bem-sucedido.

    O ReceiptHandle é um token temporário gerado pelo SQS no momento do
    recebimento. Ele identifica aquela instância específica de recebimento.

    Deletar a mensagem é importante, pois sem o delete, a mensagem
    volta a ficar visível na fila após o VisibilityTimeout (30s por padrão).
    Isso garantiria que, se o consumidor travar antes de processar,
    outro consumidor possa tentar. Mas após o processamento bem-sucedido,
    devemos deletar para evitar processamento duplicado.

    Args:
        queue_url: URL da fila de onde a mensagem foi recebida.
        receipt_handle: Token de recebimento (ReceiptHandle) da mensagem.
    """
    cliente = get_sqs_client()

    try:
        cliente.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle,
        )
        logger.info("Mensagem deletada da fila com sucesso.")
    except botocore.exceptions.ClientError as e:
        codigo = e.response["Error"]["Code"]
        logger.error(f"Erro ao deletar mensagem: [{codigo}] {e.response['Error']['Message']}")


def executar_loop_consumo(
    queue_url: str,
    tempo_espera: int = 10,
    simular_falha: bool = False,
) -> None:
    """Loop principal de consumo contínuo de mensagens.

    Fica em loop recebendo mensagens até ser interrompido com Ctrl+C.
    Para cada mensagem, realiza o fluxo: recebe -> processa -> deleta (se for bem sucedido).

    Args:
        queue_url: URL da fila a consumir.
        tempo_espera: Segundos de long polling entre chamadas.
        simular_falha: Se True, não deleta mensagens (simula falhas para DLQ).
    """
    nome_fila = queue_url.split("/")[-1]
    logger.info(f"Iniciando consumidor | Fila: {nome_fila} | Ctrl+C para encerrar.")

    try:
        while True:
            mensagens = receber_mensagens(queue_url, tempo_espera=tempo_espera)

            if not mensagens:
                logger.info("Nenhuma mensagem disponível. Aguardando...")
                continue

            for mensagem in mensagens:
                sucesso = processar_mensagem(mensagem, simular_falha=simular_falha)

                if sucesso:
                    # Só deleta após processamento confirmado.
                    # Se a aplicação travar aqui, a mensagem volta após o timeout.
                    deletar_mensagem(queue_url, mensagem["ReceiptHandle"])
                else:
                    logger.warning(
                        "Mensagem não deletada intencionalmente — "
                        "voltará à fila após o VisibilityTimeout."
                    )

    except KeyboardInterrupt:
        # Captura Ctrl+C para encerramento limpo sem stack trace
        logger.info("Consumidor encerrado pelo usuário (Ctrl+C).")


def main() -> None:
    """Interface de linha de comando para o consumidor."""
    parser = argparse.ArgumentParser(
        description="Consumidor de mensagens SQS com long polling."
    )
    parser.add_argument(
        "--fila",
        choices=["standard", "fifo"],
        required=True,
        help="Tipo de fila a consumir.",
    )
    parser.add_argument(
        "--simular-falha",
        action="store_true",
        help="Não deletar mensagens (simula falhas para demonstrar a DLQ).",
    )

    args = parser.parse_args()

    cfg = carregar_configuracao_completa()
    url_fila = cfg.url_fila_standard if args.fila == "standard" else cfg.url_fila_fifo

    executar_loop_consumo(
        queue_url=url_fila,
        tempo_espera=cfg.tempo_espera_segundos,
        simular_falha=args.simular_falha,
    )


if __name__ == "__main__":
    main()
