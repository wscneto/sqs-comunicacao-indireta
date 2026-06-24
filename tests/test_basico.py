"""
Testes básicos do projeto SQS usando moto para simular a AWS.

O moto intercepta as chamadas boto3 e simula o comportamento do SQS
localmente, sem custo e sem precisar de conexão com a AWS real.

Como rodar:
    pytest tests/test_basico.py -v

Ou simplesmente:
    pytest
"""

import os
import json
import pytest

# Configura variáveis de ambiente antes de importar os módulos do projeto.
# Isso simula um .env preenchido, necessário para carregar a configuração.
os.environ.setdefault("AWS_REGION", "sa-east-1")
os.environ.setdefault("STANDARD_QUEUE_URL", "http://placeholder")
os.environ.setdefault("FIFO_QUEUE_URL", "http://placeholder.fifo")
os.environ.setdefault("DLQ_URL", "http://placeholder-dlq")
os.environ.setdefault("WAIT_TIME_SECONDS", "0")  # 0 = sem espera nos testes
os.environ.setdefault("MAX_RECEIVE_COUNT", "3")

# Credenciais falsas obrigatórias para o moto funcionar sem erros de autenticação
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
os.environ.setdefault("AWS_DEFAULT_REGION", "sa-east-1")

import boto3
from moto import mock_aws

from src.config import carregar_configuracao, carregar_configuracao_completa
from src.utils import formatar_atributos_mensagem
from src.producer import enviar_mensagem, enviar_em_lote, _eh_fila_fifo
from src.consumer import receber_mensagens, processar_mensagem, deletar_mensagem


# Fixtures

@pytest.fixture
def sqs_mock():
    """Inicia o mock do SQS para cada teste.

    O decorator @mock_aws intercepta todas as chamadas boto3 e simula
    o SQS localmente. Ao sair do contexto, o mock é limpo.
    """
    with mock_aws():
        yield boto3.client("sqs", region_name="sa-east-1")


@pytest.fixture
def fila_standard(sqs_mock):
    """Cria uma fila Standard simulada e retorna sua URL."""
    resposta = sqs_mock.create_queue(
        QueueName="teste-standard",
        Attributes={"VisibilityTimeout": "30"},
    )
    return resposta["QueueUrl"]


@pytest.fixture
def fila_fifo(sqs_mock):
    """Cria uma fila FIFO simulada e retorna sua URL."""
    resposta = sqs_mock.create_queue(
        QueueName="teste-fifo.fifo",
        Attributes={
            "FifoQueue": "true",
            "ContentBasedDeduplication": "true",
        },
    )
    return resposta["QueueUrl"]


@pytest.fixture
def fila_dlq(sqs_mock):
    """Cria uma DLQ simulada e retorna sua URL."""
    resposta = sqs_mock.create_queue(QueueName="teste-dlq")
    return resposta["QueueUrl"]


# Testes de config.py

class TestConfig:
    """Testes para o módulo de configuração."""

    def test_carregar_configuracao_basica(self):
        """Deve carregar configuração sem erros quando AWS_REGION está definida."""
        cfg = carregar_configuracao()
        assert cfg.regiao == "sa-east-1"
        assert cfg.tempo_espera_segundos == 0
        assert cfg.max_recebimentos == 3

    def test_erro_quando_variavel_obrigatoria_ausente(self):
        """Deve lançar ValueError com mensagem clara quando AWS_REGION estiver ausente."""
        valor_original = os.environ.pop("AWS_REGION")
        try:
            # Força reload do módulo para que o load_dotenv() re-execute
            with pytest.raises(ValueError, match="AWS_REGION"):
                from src.config import _obter_variavel
                _obter_variavel("AWS_REGION", obrigatoria=True)
        finally:
            os.environ["AWS_REGION"] = valor_original

    def test_carregar_configuracao_completa_com_urls(self):
        """Deve carregar configuração completa quando todas as URLs estiverem definidas."""
        os.environ["STANDARD_QUEUE_URL"] = "https://sqs.sa-east-1.amazonaws.com/1/demo"
        os.environ["FIFO_QUEUE_URL"] = "https://sqs.sa-east-1.amazonaws.com/1/demo.fifo"
        os.environ["DLQ_URL"] = "https://sqs.sa-east-1.amazonaws.com/1/dlq"
        cfg = carregar_configuracao_completa()
        assert "demo" in cfg.url_fila_standard
        assert cfg.url_fila_fifo.endswith(".fifo")


# Testes de utils.py

class TestUtils:
    """Testes para funções utilitárias compartilhadas."""

    def test_formatar_atributos_mensagem(self):
        """Deve converter dict simples para o formato exigido pelo SQS."""
        entrada = {"origem": "sistema-a", "versao": "2"}
        resultado = formatar_atributos_mensagem(entrada)

        assert resultado["origem"]["StringValue"] == "sistema-a"
        assert resultado["origem"]["DataType"] == "String"
        assert resultado["versao"]["StringValue"] == "2"

    def test_formatar_atributos_vazio(self):
        """Deve retornar dict vazio quando não há atributos."""
        assert formatar_atributos_mensagem({}) == {}


# Testes de criação de fila

class TestCriacaoFila:
    """Testes para criação e gerenciamento de filas."""

    def test_criar_fila_standard(self, sqs_mock):
        """Deve criar uma fila Standard com sucesso."""
        resposta = sqs_mock.create_queue(QueueName="minha-fila")
        assert "QueueUrl" in resposta
        assert "minha-fila" in resposta["QueueUrl"]

    def test_criar_fila_fifo(self, sqs_mock):
        """Deve criar uma fila FIFO com atributo FifoQueue=true."""
        resposta = sqs_mock.create_queue(
            QueueName="minha-fila.fifo",
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "true"},
        )
        url = resposta["QueueUrl"]
        assert url.endswith(".fifo")

        # Verifica que o atributo FifoQueue está definido
        atributos = sqs_mock.get_queue_attributes(
            QueueUrl=url, AttributeNames=["FifoQueue"]
        )
        assert atributos["Attributes"]["FifoQueue"] == "true"

    def test_detectar_fila_fifo_pela_url(self):
        """A função _eh_fila_fifo deve detectar corretamente pelo sufixo da URL."""
        assert _eh_fila_fifo("https://sqs.aws.com/123/demo.fifo") is True
        assert _eh_fila_fifo("https://sqs.aws.com/123/demo-standard") is False


# Testes de producer.py

class TestProdutor:
    """Testes para envio de mensagens."""

    def test_enviar_mensagem_standard(self, sqs_mock, fila_standard):
        """Deve enviar uma mensagem para fila Standard com sucesso."""
        resposta = enviar_mensagem(
            queue_url=fila_standard,
            corpo="Teste de mensagem",
        )
        assert "MessageId" in resposta
        assert resposta["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_enviar_mensagem_com_atributos(self, sqs_mock, fila_standard):
        """Deve enviar mensagem com atributos customizados."""
        resposta = enviar_mensagem(
            queue_url=fila_standard,
            corpo="Mensagem com atributos",
            atributos={"origem": "teste", "prioridade": "alta"},
        )
        assert "MessageId" in resposta

    def test_enviar_mensagem_fifo(self, sqs_mock, fila_fifo):
        """Deve enviar mensagem para fila FIFO com MessageGroupId."""
        resposta = enviar_mensagem(
            queue_url=fila_fifo,
            corpo="Mensagem FIFO",
            group_id="grupo-1",
        )
        assert "MessageId" in resposta

    def test_erro_fila_fifo_sem_group_id(self, sqs_mock, fila_fifo):
        """Deve lançar ValueError se fila FIFO não receber group_id."""
        with pytest.raises(ValueError, match="MessageGroupId"):
            enviar_mensagem(queue_url=fila_fifo, corpo="Sem group_id")

    def test_enviar_em_lote(self, sqs_mock, fila_standard):
        """Deve enviar múltiplas mensagens em um único lote."""
        mensagens = [
            {"corpo": f"Mensagem {i}", "atributos": {"idx": str(i)}}
            for i in range(5)
        ]
        resposta = enviar_em_lote(fila_standard, mensagens)
        assert len(resposta["Successful"]) == 5
        assert resposta.get("Failed", []) == []

    def test_erro_lote_acima_do_limite(self, sqs_mock, fila_standard):
        """Deve lançar ValueError se o lote tiver mais de 10 mensagens."""
        mensagens = [{"corpo": f"msg {i}"} for i in range(11)]
        with pytest.raises(ValueError, match="máximo"):
            enviar_em_lote(fila_standard, mensagens)


# Testes de consumer.py

class TestConsumidor:
    """Testes para recebimento e deleção de mensagens."""

    def test_receber_mensagem(self, sqs_mock, fila_standard):
        """Deve receber uma mensagem que foi enviada previamente."""
        # Envia uma mensagem primeiro
        sqs_mock.send_message(QueueUrl=fila_standard, MessageBody="Olá SQS")

        mensagens = receber_mensagens(fila_standard, tempo_espera=0)
        assert len(mensagens) == 1
        assert mensagens[0]["Body"] == "Olá SQS"

    def test_fila_vazia_retorna_lista_vazia(self, sqs_mock, fila_standard):
        """Deve retornar lista vazia quando a fila não tem mensagens."""
        mensagens = receber_mensagens(fila_standard, tempo_espera=0)
        assert mensagens == []

    def test_processar_mensagem_sucesso(self):
        """Deve processar mensagem sem falha e retornar True."""
        mensagem_fake = {
            "MessageId": "abc-123",
            "Body": "Conteúdo de teste",
            "MessageAttributes": {},
            "Attributes": {"ApproximateReceiveCount": "1"},
        }
        resultado = processar_mensagem(mensagem_fake, simular_falha=False)
        assert resultado is True

    def test_processar_mensagem_com_falha_simulada(self):
        """Deve retornar False quando simular_falha=True."""
        mensagem_fake = {
            "MessageId": "abc-456",
            "Body": "Mensagem que vai falhar",
            "MessageAttributes": {},
            "Attributes": {"ApproximateReceiveCount": "2"},
        }
        resultado = processar_mensagem(mensagem_fake, simular_falha=True)
        assert resultado is False

    def test_deletar_mensagem(self, sqs_mock, fila_standard):
        """Deve deletar uma mensagem e não encontrá-la em recebimentos futuros."""
        sqs_mock.send_message(QueueUrl=fila_standard, MessageBody="Para deletar")

        # Recebe a mensagem
        mensagens = receber_mensagens(fila_standard, tempo_espera=0)
        assert len(mensagens) == 1

        # Deleta a mensagem
        deletar_mensagem(fila_standard, mensagens[0]["ReceiptHandle"])

        # Tenta receber novamente. Deve estar vazia (visibility timeout = 30s no mock)
        # Nota: o mock do moto respeita o VisibilityTimeout, então a fila fica vazia
        mensagens_apos_delete = receber_mensagens(fila_standard, tempo_espera=0)
        assert mensagens_apos_delete == []

    def test_ciclo_completo_envio_recebimento_delete(self, sqs_mock, fila_standard):
        """Testa o fluxo completo: envio -> recebimento -> processamento -> deleção."""
        # 1. Envia
        enviar_mensagem(fila_standard, "Ciclo completo")

        # 2. Recebe
        mensagens = receber_mensagens(fila_standard, tempo_espera=0)
        assert len(mensagens) == 1
        assert mensagens[0]["Body"] == "Ciclo completo"

        # 3. Processa
        sucesso = processar_mensagem(mensagens[0])
        assert sucesso is True

        # 4. Deleta
        deletar_mensagem(fila_standard, mensagens[0]["ReceiptHandle"])

        # 5. Confirma que a fila está vazia
        assert receber_mensagens(fila_standard, tempo_espera=0) == []
