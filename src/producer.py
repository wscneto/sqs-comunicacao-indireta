"""
Módulo produtor de mensagens SQS.

Responsável por enviar mensagens para filas Standard e FIFO.
Demonstra envio simples, envio em lote e uso de atributos customizados.

Uso via linha de comando:
    python -m src.producer --fila standard --corpo "Olá SQS" --quantidade 3
    python -m src.producer --fila fifo --corpo "Pedido #42" --grupo pedidos
"""

import argparse
import sys
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import botocore.exceptions

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import carregar_configuracao_completa
from src.utils import get_sqs_client, configurar_logger, formatar_atributos_mensagem

logger = configurar_logger(__name__)

# Máximo de mensagens por chamada send_message_batch (limite da API SQS)
TAMANHO_MAXIMO_LOTE = 10


def _eh_fila_fifo(url_fila: str) -> bool:
    """Verifica se a URL pertence a uma fila FIFO.

    Filas FIFO têm nome terminando em '.fifo', e isso reflete na URL.

    Args:
        url_fila: URL da fila SQS.

    Returns:
        True se for fila FIFO, False caso contrário.
    """
    return url_fila.endswith(".fifo")


def enviar_mensagem(
    queue_url: str,
    corpo: str,
    atributos: Optional[dict[str, str]] = None,
    group_id: Optional[str] = None,
    dedup_id: Optional[str] = None,
) -> dict:
    """Envia uma única mensagem para uma fila SQS.

    Detecta automaticamente se a fila é FIFO e exige MessageGroupId nesse caso.
    Filas FIFO agrupam mensagens por GroupId para garantir a ordem de entrega
    dentro de cada grupo.

    Args:
        queue_url: URL da fila de destino.
        corpo: Corpo da mensagem (string de até 256 KB).
        atributos: Atributos customizados no formato chave-valor simples.
        group_id: MessageGroupId — obrigatório para filas FIFO.
                  Mensagens com o mesmo group_id são entregues na ordem de envio.
        dedup_id: MessageDeduplicationId — identificador único para evitar
                  duplicatas em filas FIFO. Se None e a fila usar
                  ContentBasedDeduplication, o SQS gera automaticamente.

    Returns:
        Resposta da API SQS com MessageId e outras metainformações.

    Raises:
        ValueError: Se a fila for FIFO e group_id não for fornecido.
        SystemExit: Em caso de erro na chamada à AWS.
    """
    cliente = get_sqs_client()

    # Filas FIFO requerem MessageGroupId para garantir ordenação
    if _eh_fila_fifo(queue_url) and not group_id:
        raise ValueError(
            "Filas FIFO requerem um 'group_id' (MessageGroupId). "
            "Use --grupo ao chamar via linha de comando."
        )

    kwargs: dict = {
        "QueueUrl": queue_url,
        "MessageBody": corpo,
    }

    if atributos:
        kwargs["MessageAttributes"] = formatar_atributos_mensagem(atributos)

    if _eh_fila_fifo(queue_url):
        kwargs["MessageGroupId"] = group_id
        if dedup_id:
            kwargs["MessageDeduplicationId"] = dedup_id

    try:
        resposta = cliente.send_message(**kwargs)
        logger.info(
            f"Mensagem enviada | ID: {resposta['MessageId']} | "
            f"Fila: {queue_url.split('/')[-1]}"
        )
        return resposta
    except botocore.exceptions.ClientError as e:
        codigo = e.response["Error"]["Code"]
        logger.error(f"Erro ao enviar mensagem: [{codigo}] {e.response['Error']['Message']}")
        sys.exit(1)


def enviar_em_lote(queue_url: str, mensagens: list[dict]) -> dict:
    """Envia até 10 mensagens em uma única chamada (send_message_batch).

    O envio em lote reduz o número de chamadas à API e, consequentemente,
    o custo, tendo em vista que o SQS cobra por chamada, não por mensagem.

    Cada elemento de 'mensagens' deve ser um dict com as chaves:
        - corpo (str): conteúdo da mensagem
        - atributos (dict, opcional): atributos customizados
        - group_id (str, opcional): obrigatório para filas FIFO
        - dedup_id (str, opcional): para deduplicação em filas FIFO

    Args:
        queue_url: URL da fila de destino.
        mensagens: Lista de dicionários descrevendo cada mensagem.

    Returns:
        Resposta da API com listas de Successful e Failed.

    Raises:
        ValueError: Se a lista exceder 10 mensagens (limite do SQS).
    """
    if len(mensagens) > TAMANHO_MAXIMO_LOTE:
        raise ValueError(
            f"O SQS aceita no máximo {TAMANHO_MAXIMO_LOTE} mensagens por lote. "
            f"Recebido: {len(mensagens)}."
        )

    cliente = get_sqs_client()

    entradas = []
    for idx, msg in enumerate(mensagens):
        entrada: dict = {
            # Id único dentro do lote. Não é o MessageId do SQS
            "Id": str(idx),
            "MessageBody": msg["corpo"],
        }

        if msg.get("atributos"):
            entrada["MessageAttributes"] = formatar_atributos_mensagem(msg["atributos"])

        if _eh_fila_fifo(queue_url):
            if not msg.get("group_id"):
                raise ValueError(f"Mensagem {idx} para fila FIFO precisa de 'group_id'.")
            entrada["MessageGroupId"] = msg["group_id"]
            if msg.get("dedup_id"):
                entrada["MessageDeduplicationId"] = msg["dedup_id"]

        entradas.append(entrada)

    try:
        resposta = cliente.send_message_batch(
            QueueUrl=queue_url,
            Entries=entradas,
        )
        enviadas = len(resposta.get("Successful", []))
        falhas = len(resposta.get("Failed", []))
        logger.info(
            f"Lote enviado | Sucesso: {enviadas} | Falhas: {falhas} | "
            f"Fila: {queue_url.split('/')[-1]}"
        )
        if falhas > 0:
            for falha in resposta["Failed"]:
                logger.warning(f"Falha no item {falha['Id']}: {falha['Message']}")
        return resposta
    except botocore.exceptions.ClientError as e:
        codigo = e.response["Error"]["Code"]
        logger.error(f"Erro no envio em lote: [{codigo}] {e.response['Error']['Message']}")
        sys.exit(1)


def main() -> None:
    """Interface de linha de comando para envio de mensagens."""
    parser = argparse.ArgumentParser(
        description="Produtor de mensagens SQS que envia para filas Standard ou FIFO."
    )
    parser.add_argument(
        "--fila",
        choices=["standard", "fifo"],
        required=True,
        help="Tipo de fila de destino.",
    )
    parser.add_argument(
        "--corpo",
        default="Mensagem de teste",
        help="Conteúdo da mensagem (padrão: 'Mensagem de teste').",
    )
    parser.add_argument(
        "--quantidade",
        type=int,
        default=1,
        help="Número de mensagens a enviar (padrão: 1).",
    )
    parser.add_argument(
        "--grupo",
        default="grupo-demo",
        help="MessageGroupId para filas FIFO (padrão: 'grupo-demo').",
    )
    parser.add_argument(
        "--lote",
        action="store_true",
        help="Envia todas as mensagens em um único lote (até 10).",
    )

    args = parser.parse_args()

    cfg = carregar_configuracao_completa()
    url_fila = cfg.url_fila_standard if args.fila == "standard" else cfg.url_fila_fifo

    logger.info(
        f"Iniciando produtor | Fila: {args.fila} | "
        f"Quantidade: {args.quantidade} | Lote: {args.lote}"
    )

    # Atributos comuns a todas as mensagens, para demonstrar o recurso
    atributos_base = {
        "origem": "producer-demo",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if args.lote:
        if args.quantidade > TAMANHO_MAXIMO_LOTE:
            logger.error(f"Modo lote suporta no máximo {TAMANHO_MAXIMO_LOTE} mensagens.")
            sys.exit(1)
        mensagens = [
            {
                "corpo": f"{args.corpo} [{i + 1}/{args.quantidade}]",
                "atributos": atributos_base,
                "group_id": args.grupo,
                "dedup_id": str(uuid.uuid4()),
            }
            for i in range(args.quantidade)
        ]
        enviar_em_lote(url_fila, mensagens)
    else:
        for i in range(args.quantidade):
            corpo = f"{args.corpo} [{i + 1}/{args.quantidade}]"
            enviar_mensagem(
                queue_url=url_fila,
                corpo=corpo,
                atributos=atributos_base,
                group_id=args.grupo,
                dedup_id=str(uuid.uuid4()),
            )

    logger.info("Produtor finalizado.")


if __name__ == "__main__":
    main()
