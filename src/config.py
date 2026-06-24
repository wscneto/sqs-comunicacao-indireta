"""
Módulo de configuração do projeto SQS.

Carrega variáveis do arquivo .env e expõe como constantes tipadas.
Valida na inicialização se as variáveis obrigatórias estão presentes,
evitando erros silenciosos em tempo de execução.
"""

import os
from dataclasses import dataclass
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env para os.environ
# Se o .env não existir, os valores devem vir do ambiente (ex.: CI/CD)
load_dotenv()


def _obter_variavel(nome: str, obrigatoria: bool = True) -> str:
    """Lê uma variável de ambiente, lançando exceção clara se alguma variável obrigatória estiver ausente.

    Args:
        nome: Nome da variável de ambiente.
        obrigatoria: Se True, lança ValueError quando a variável não está definida.

    Returns:
        O valor da variável como string.

    Raises:
        ValueError: Quando a variável obrigatória está ausente ou vazia.
    """
    valor = os.getenv(nome, "").strip()
    if obrigatoria and not valor:
        raise ValueError(
            f"Variável de ambiente obrigatória ausente: '{nome}'. "
            f"Copie o .env.example para .env e preencha o valor de '{nome}'."
        )
    return valor


def _obter_inteiro(nome: str, padrao: int) -> int:
    """Lê uma variável de ambiente como inteiro, usando valor padrão se ausente.

    Args:
        nome: Nome da variável de ambiente.
        padrao: Valor padrão caso a variável não esteja definida.

    Returns:
        O valor da variável como int.
    """
    valor_str = os.getenv(nome, "").strip()
    if not valor_str:
        return padrao
    try:
        return int(valor_str)
    except ValueError:
        raise ValueError(
            f"A variável '{nome}' deve ser um inteiro, mas recebeu: '{valor_str}'."
        )


@dataclass(frozen=True)
class Configuracao:
    """Configurações centralizadas do projeto.

    Usa dataclass com frozen=True para garantir imutabilidade.
    As configurações não devem mudar durante a execução.
    """

    regiao: str
    url_fila_standard: str
    url_fila_fifo: str
    url_dlq: str
    tempo_espera_segundos: int
    max_recebimentos: int


def carregar_configuracao() -> Configuracao:
    """Carrega e valida todas as configurações a partir do ambiente.

    As URLs das filas são obrigatórias para os scripts de produção,
    mas opcionais para testes (que criam filas simuladas via moto).

    Returns:
        Instância de Configuracao com todos os valores preenchidos.

    Raises:
        ValueError: Se alguma variável obrigatória estiver ausente.
    """
    return Configuracao(
        regiao=_obter_variavel("AWS_REGION"),
        url_fila_standard=_obter_variavel("STANDARD_QUEUE_URL", obrigatoria=False),
        url_fila_fifo=_obter_variavel("FIFO_QUEUE_URL", obrigatoria=False),
        url_dlq=_obter_variavel("DLQ_URL", obrigatoria=False),
        tempo_espera_segundos=_obter_inteiro("WAIT_TIME_SECONDS", padrao=10),
        max_recebimentos=_obter_inteiro("MAX_RECEIVE_COUNT", padrao=3),
    )


def carregar_configuracao_completa() -> Configuracao:
    """Carrega configuração exigindo que todas as URLs de fila estejam preenchidas.

    Use esta função nos scripts de produção que precisam das URLs reais.
    Para testes com moto, use carregar_configuracao().

    Returns:
        Instância de Configuracao com todos os valores preenchidos.

    Raises:
        ValueError: Se qualquer variável (incluindo URLs de filas) estiver ausente.
    """
    return Configuracao(
        regiao=_obter_variavel("AWS_REGION"),
        url_fila_standard=_obter_variavel("STANDARD_QUEUE_URL"),
        url_fila_fifo=_obter_variavel("FIFO_QUEUE_URL"),
        url_dlq=_obter_variavel("DLQ_URL"),
        tempo_espera_segundos=_obter_inteiro("WAIT_TIME_SECONDS", padrao=10),
        max_recebimentos=_obter_inteiro("MAX_RECEIVE_COUNT", padrao=3),
    )
