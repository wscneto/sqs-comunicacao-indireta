# Conceitos: Amazon SQS e Comunicação Indireta

## O que é o Amazon SQS?

O Amazon Simple Queue Service (SQS) é um serviço gerenciado de filas de mensagens
que permite desacoplar componentes de sistemas distribuídos. Em vez de um serviço A
chamar o serviço B diretamente (comunicação síncrona), A deposita uma mensagem na
fila e B a consome quando estiver disponível (comunicação assíncrona).

**Vantagens do desacoplamento:**
- Se o consumidor cair, as mensagens ficam na fila até ele voltar.
- O produtor não precisa esperar o consumidor processar.
- É possível escalar produtores e consumidores independentemente.

---

## Fila Standard vs. Fila FIFO

### Fila Standard

- **Ordem**: Melhor esforço (*best-effort ordering*) - sem garantia de ordem.
- **Entrega**: *At-least-once* - a mesma mensagem pode ser entregue mais de uma vez
  (raro, mas possível em caso de falha de rede interna).
- **Throughput**: Praticamente ilimitado - ideal para alta vazão de mensagens.
- **Caso de uso**: Processamento de tarefas independentes onde a ordem e duplicatas
  não são críticas (ex.: envio de e-mails, geração de thumbnails).

### Fila FIFO (First-In, First-Out)

- **Ordem**: Garantida dentro de cada `MessageGroupId`. Mensagens com o mesmo
  GroupId são entregues na exata ordem em que foram enviadas.
- **Entrega**: *Exactly-once* - cada mensagem é entregue exatamente uma vez.
- **Throughput**: Limitado a 300 TPS (transações por segundo) por padrão,
  ou 3.000 TPS com batching.
- **Caso de uso**: Processamento de pedidos, transações financeiras, sequências
  de comandos onde a ordem importa.

**Regra**: o nome da fila FIFO **obrigatoriamente** termina em `.fifo`.

---

## Garantias de Entrega

| Garantia | Fila Standard | Fila FIFO |
|---|---|---|
| Entrega garantida | Sim (at-least-once) | Sim (exactly-once) |
| Sem duplicatas | Não garantido | Garantido |
| Ordem garantida | Não | Sim (por grupo) |

**At-least-once** significa que o SQS garante que a mensagem será entregue pelo
menos uma vez. Se o consumidor não confirmar o recebimento a tempo, o SQS a
reentrega. Por isso, consumidores de filas Standard devem ser **idempotentes**
(processar a mesma mensagem duas vezes não causa efeito colateral).

---

## Visibility Timeout

O `VisibilityTimeout` é o período (em segundos) durante o qual uma mensagem fica
**invisível** para outros consumidores após ser recebida.

**Por que existe?**
Quando o consumidor A recebe uma mensagem, ela não é imediatamente deletada da fila
- ela fica oculta. Isso garante que, se A travar ou cair antes de processar,
a mensagem voltará a ficar visível e outro consumidor poderá tentar processá-la.

**Fluxo correto:**
1. Consumidor recebe mensagem (ela fica invisível por 30s).
2. Consumidor processa.
3. Consumidor chama `delete_message` → mensagem é removida definitivamente.
4. Se o consumidor travar, após 30s a mensagem reaparece para nova tentativa.

**Valor padrão**: 30 segundos. **Máximo**: 12 horas.

---

## Política de Retenção

O `MessageRetentionPeriod` define por quanto tempo o SQS mantém uma mensagem
na fila antes de descartá-la automaticamente.

- **Mínimo**: 60 segundos (1 minuto)
- **Padrão**: 345.600 segundos (4 dias)
- **Máximo**: 1.209.600 segundos (14 dias)

Neste projeto usamos 4 dias para as filas principais e 14 dias para a DLQ,
pois queremos mais tempo para investigar mensagens que falharam.

---

## Dead-Letter Queue (DLQ)

A DLQ é uma fila especial que recebe mensagens que falharam no processamento
mais vezes do que o limite configurado (`maxReceiveCount`).

### Como funciona:

```
Fila Principal
    Mensagem recebida → falha → invisível por 30s → visível novamente
    Repetido N vezes (maxReceiveCount)...
    Na (N+1)-ésima tentativa: SQS move automaticamente para a DLQ
```

### Por que usar DLQ?

Sem DLQ, uma mensagem "envenenada" (ex.: dados inválidos que sempre causam
exceção no consumidor) ficaria em loop eterno na fila principal:
- Consumindo chamadas à API (custo).
- Atrasando o processamento de outras mensagens.
- Impossível distinguir mensagens saudáveis das problemáticas.

Com DLQ:
- Mensagens problemáticas são isoladas automaticamente.
- A fila principal continua funcionando normalmente.
- A equipe pode investigar as mensagens na DLQ com calma.

### Configuração (RedrivePolicy):

```json
{
    "maxReceiveCount": "3",
    "deadLetterTargetArn": "arn:aws:sqs:regiao:conta:nome-da-dlq"
}
```

### Long Polling

O `ReceiveMessageWaitTimeSeconds` controla o comportamento de polling:

- **Short polling (0s)**: retorna imediatamente, mesmo se a fila estiver vazia.
  Gera muitas chamadas desnecessárias e aumenta o custo.
- **Long polling (1-20s)**: aguarda até N segundos por mensagens antes de retornar.
  Reduz chamadas vazias, diminui latência de entrega e economiza dinheiro.

Neste projeto usamos `WaitTimeSeconds=10` por padrão, configurável no `.env`.
