# Relatório Final - Comunicação Indireta com Amazon SQS

**Disciplina:** Sistemas Distribuídos  
**Tema:** Comunicação Indireta com Amazon SQS  
**Integrantes:**
- João Pedro Simões da Silva Sousa
- João Victor Cavalcante da Silva Correia
- Marlos Balbino Nunes
- Walter Soares Costa Neto

**Data:** 26/06/2026

---

## 1. Introdução

A comunicação indireta é um padrão fundamental em sistemas distribuídos que
permite que componentes troquem informações sem estarem diretamente acoplados
em tempo ou espaço. Neste trabalho, exploramos o Amazon Simple Queue Service
(SQS) como mecanismo de comunicação assíncrona baseada em filas.

O SQS é um serviço gerenciado da AWS que elimina a necessidade de manter
infraestrutura de mensageria própria, oferecendo alta disponibilidade,
escalabilidade automática e garantias de entrega configuráveis.

---

## 2. Objetivos

- Demonstrar comunicação assíncrona e desacoplada entre produtores e
  consumidores usando Amazon SQS.
- Comparar e contrastar filas **Standard** e **FIFO** em termos de garantias.
- Implementar e demonstrar a funcionalidade de **Dead-Letter Queue (DLQ)**.
- Explorar conceitos de *visibility timeout* e *long polling*.
- Aplicar boas práticas de desenvolvimento: configuração externalizada,
  infraestrutura como código, testes automatizados.

---

## 3. Desenvolvimento

### 3.1 Infraestrutura

A infraestrutura de filas foi criada programaticamente via `scripts/create_queues.py`,
demonstrando o conceito de *Infraestrutura como Código (IaC)*. O script cria:

- **`demo-standard`**: Fila Standard com `VisibilityTimeout=30s`,
  `MessageRetentionPeriod=4 dias` e `RedrivePolicy` para a DLQ após 3 falhas.
- **`demo-fifo.fifo`**: Fila FIFO com `ContentBasedDeduplication=true` e
  configurações equivalentes.
- **`demo-dlq`**: Dead-Letter Queue Standard (DLQ da fila Standard), com
  `MessageRetentionPeriod=14 dias` para análise prolongada das falhas.
- **`demo-dlq.fifo`**: Dead-Letter Queue FIFO (DLQ da fila FIFO).

> **Lição aprendida:** o SQS exige que a DLQ seja do **mesmo tipo** da fila de
> origem. A tentativa inicial de usar uma única DLQ Standard para ambas as
> filas falhou com `InvalidParameterValue: Dead-letter queue must be same type
> of queue as the source`. Por isso foram criadas duas DLQs - uma Standard e
> uma FIFO.

### 3.2 Produtor (`src/producer.py`)

O módulo produtor implementa:
- Envio de mensagem única com `send_message`.
- Envio em lote de até 10 mensagens com `send_message_batch`.
- Detecção automática de fila FIFO pelo sufixo `.fifo` na URL.
- Suporte a atributos de mensagem customizados.

### 3.3 Consumidor (`src/consumer.py`)

O módulo consumidor implementa:
- Recebimento com **long polling** (`WaitTimeSeconds=10`).
- Ciclo completo: receber -> processar -> deletar.
- Encerramento limpo via `Ctrl+C`.
- Modo de falha simulada para demonstrar a DLQ.

### 3.4 Dead-Letter Queue (`scripts/demo_dlq.py`)

O script de demonstração:
1. Envia uma mensagem para a fila principal.
2. Recebe a mensagem repetidamente **sem deletar** (simulando falha).
3. Aguarda o SQS mover a mensagem para a DLQ após `maxReceiveCount=3`.
4. Confirma o recebimento na DLQ.

### 3.5 Testes

Os testes em `tests/test_basico.py` usam a biblioteca **moto** para simular
o SQS localmente, sem custo e sem conexão com a AWS. Foram implementados
20 casos de teste cobrindo:
- Validação de configuração.
- Criação de filas Standard e FIFO.
- Envio simples e em lote.
- Recebimento com long polling.
- Ciclo completo de processamento.

---

## 4. Resultados

| Critério | Status |
|---|---|
| Criação de filas via código | ✅ |
| Envio de mensagens (simples, lote, atributos) | ✅ |
| Consumo com long polling | ✅ |
| Demonstração da DLQ | ✅ |
| 20 testes passando com moto | ✅ |
| Type hints e docstrings em PT-BR | ✅ |

### Observações sobre Fila Standard vs. FIFO

Durante os testes na AWS (região `sa-east-1`), ambas as filas entregaram as
mensagens corretamente no ciclo *enviar -> receber -> processar -> deletar*.
Diferenças observadas na prática:

- A fila **FIFO** exigiu o parâmetro `MessageGroupId` em todo envio; sem ele,
  o `send_message` falha. A deduplicação por conteúdo (`ContentBasedDeduplication`)
  dispensou o envio manual de `MessageDeduplicationId`.
- A fila **Standard** aceitou envios sem nenhum parâmetro de ordenação/grupo,
  refletindo seu modelo de maior throughput e ordem não garantida.
- Os atributos de mensagem (`origem`, `timestamp`) foram preservados e lidos
  pelo consumidor em ambos os tipos de fila.

### Comportamento da DLQ

A execução de `scripts/demo_dlq.py` confirmou o redirecionamento automático:
após a mensagem ser recebida 3 vezes sem ser deletada (`ApproximateReceiveCount`
chegando a 3, conforme `maxReceiveCount=3`), na tentativa seguinte ela não
retornou mais à fila principal - o SQS a moveu para a DLQ, onde foi localizada
com o mesmo `MessageId` e corpo originais. Para acelerar a demonstração, o
`VisibilityTimeout` da fila foi temporariamente reduzido para 1s, fazendo a
mensagem reaparecer rapidamente entre as tentativas.

---

## 5. Conclusão

A implementação demonstrou que o Amazon SQS simplifica significativamente a
comunicação assíncrona em sistemas distribuídos. A abstração da fila permite
que produtores e consumidores evoluam independentemente, e recursos como DLQ
e visibility timeout fornecem robustez sem complexidade adicional no código
de aplicação.

A diferença entre filas Standard e FIFO evidencia o trade-off clássico em
sistemas distribuídos: **consistência vs. disponibilidade/throughput**. A escolha
do tipo de fila deve ser guiada pelos requisitos de negócio da aplicação.

---

## 6. Referências

- [Documentação oficial Amazon SQS](https://docs.aws.amazon.com/sqs/)
- [boto3 SQS Reference](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html)
- [moto - Mock AWS Services](https://docs.getmoto.org/)
- COULOURIS, G. et al. *Sistemas Distribuídos: Conceitos e Projeto*. 5ª ed.
