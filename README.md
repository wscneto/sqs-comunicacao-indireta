# SQS - Comunicação Indireta

Projeto da disciplina de **Sistemas Distribuídos**, ministrada pelo Prof. Leandro Sales, que
demonstra comunicação assíncrona e desacoplada usando o **Simple Queue
Service (SQS)** da AWS.

## Integrantes

- João Pedro Simões da Silva Sousa
- João Victor Cavalcante da Silva Correia
- Marlos Balbino Nunes
- Walter Soares Costa Neto

## O que este projeto demonstra

- Filas **Standard** (at-least-once, sem ordem garantida) e **FIFO** (exactly-once, ordem garantida);
- **Long polling** para consumo eficiente de mensagens;
- **Visibility timeout** e o ciclo de vida da mensagem;
- **Dead-Letter Queue (DLQ)** para isolar mensagens com falhas repetidas;
- **Infraestrutura como Código (IaC)**: criação de filas via Python/boto3;
- **Testes locais**: utilizando `moto` para simular o SQS localmente.

## Pré-requisitos

- Python 3.11+
- Conta AWS (plano gratuito é suficiente)
- AWS CLI instalado
- Credenciais IAM com política `AmazonSQSFullAccess`

## Passo a passo de instalação e execução

### 1. Clone o repositório e crie o ambiente virtual

```bash
git clone <url-do-repositório>
cd sqs-comunicacao-indireta
python3 -m venv .venv
source .venv/bin/activate   # Linux/macOS
# .venv\Scripts\activate    # Windows
```

### 2. Instale as dependências

```bash
pip install -r requirements.txt
```

### 3. Configure as credenciais AWS

```bash
aws configure
# Informe: AWS Access Key ID, Secret Access Key, região (sa-east-1), formato (json)
```

### 4. Copie e edite o arquivo de configuração

```bash
cp .env.example .env
# Deixe as URLs de fila em branco por enquanto, pois elas serão preenchidas no passo 5
```

### 5. Crie as filas SQS

```bash
python scripts/create_queues.py
```

Copie as URLs impressas e preencha no `.env`:

```
STANDARD_QUEUE_URL=https://sqs.sa-east-1.amazonaws.com/XXXX/demo-standard
FIFO_QUEUE_URL=https://sqs.sa-east-1.amazonaws.com/XXXX/demo-fifo.fifo
DLQ_URL=https://sqs.sa-east-1.amazonaws.com/XXXX/demo-dlq
```

### 6. Envie mensagens com o producer

```bash
# Enviar 1 mensagem para a fila Standard
python -m src.producer --fila standard --corpo "Olá, SQS!"

# Enviar 5 mensagens em lote para a fila Standard
python -m src.producer --fila standard --corpo "Pedido" --quantidade 5 --lote

# Enviar para a fila FIFO (parâmetro 'grupo' é obrigatório)
python -m src.producer --fila fifo --corpo "Transação" --grupo pagamentos
```

### 7. Consuma mensagens com o consumer

Abra um segundo terminal e ative o `.venv`:

```bash
# Consumir da fila Standard. Ctrl+C para encerrar.
python -m src.consumer --fila standard

# Consumir da fila FIFO
python -m src.consumer --fila fifo
```

### 8. Demonstre a Dead-Letter Queue

```bash
python scripts/demo_dlq.py
```

O script envia uma mensagem, simula 3 falhas de processamento, e confirma
que a mensagem foi redirecionada para a DLQ.

## Rodando os testes

Os testes usam `moto` para simular o SQS sem a necessidade de se conectar com a AWS:

```bash
pytest tests/test_basico.py -v
```

## Estrutura do projeto

```
sqs-comunicacao-indireta/
├── .env.example              # Modelo de configuração
├── requirements.txt          # Dependências com versões fixadas
├── docs/
│   ├── diagrams/
│   │   └── diagramas...      # Diagramas UML da arquitetura do projeto
│   ├── conceitos.md          # Conceitos fundamentais do SQS
│   └── relatorio.md          # Relatório do projeto
├── src/
│   ├── config.py             # Carrega e valida a configuração do .env
│   ├── utils.py              # Cliente SQS, logger, helpers
│   ├── producer.py           # Envio de mensagens
│   └── consumer.py           # Recebimento com long polling + deleção
├── scripts/
│   ├── create_queues.py      # Cria Standard, FIFO e DLQ via boto3
│   └── demo_dlq.py           # Demonstração da Dead-Letter Queue
└── tests/
    └── test_basico.py        # 20 testes com moto
```

## Documentação adicional

- [Documentação oficial Amazon SQS](https://docs.aws.amazon.com/sqs/)
- [boto3 SQS Reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html)
- [moto - Mock AWS Services](https://docs.getmoto.org/)
- COULOURIS, G. et al. *Sistemas Distribuídos: Conceitos e Projeto*. 5ª ed.