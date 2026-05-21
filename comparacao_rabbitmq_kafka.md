# Comparacao: RabbitMQ/AMQP vs Apache Kafka

## Contexto da Aplicacao

O sistema implementado e um pipeline de processamento de pedidos de e-commerce com as seguintes etapas:

```
producer_loja      -> queue_pedidos      -> consumer_pedidos
                                                   |
                          +-----------------------+--+-------------------+
                          |                       |                      |
                   queue_pagamentos        queue_estoque        queue_notificacoes
                          |                       |                      |
               consumer_pagamentos     consumer_estoque    consumer_notificacoes
                          |
              +-----------+------------------+
              |                              |
      queue_logistica             queue_notificacoes
              |
     consumer_logistica

producer_estoque   -> queue_estoque      -> consumer_estoque
```

Cada fila tem um consumidor com responsabilidade especifica. O fluxo e orientado a
eventos: cada consumidor pode ser tanto receptor quanto novo produtor de mensagens.

---

## 1. Modelo de Mensageria

| Aspecto              | RabbitMQ / AMQP                                           | Apache Kafka                                                 |
|----------------------|-----------------------------------------------------------|--------------------------------------------------------------|
| Paradigma            | Message broker (push): enfileira e entrega mensagens      | Log distribuido (pull): mensagens sao registros imutaveis    |
| Unidade logica       | Fila (Queue) + Exchange + Binding                         | Topico (Topic) particionado                                  |
| Retencao             | Mensagem e removida apos o ack do consumidor              | Mensagem persiste pelo tempo configurado (ex.: 7 dias)       |
| Ordenacao            | FIFO por fila; sem garantia entre filas distintas         | FIFO por particao; sem garantia entre particoes              |
| Reprocessamento      | Requer DLQ (Dead Letter Queue) ou reenvio manual          | Consumer pode resetar o offset e reler qualquer mensagem     |

---

## 2. Roteamento de Mensagens

| Aspecto              | RabbitMQ / AMQP                                           | Apache Kafka                                                 |
|----------------------|-----------------------------------------------------------|--------------------------------------------------------------|
| Mecanismo            | Exchange (direct, topic, fanout, headers) + routing key   | Producer escolhe o topico; sem roteamento no broker          |
| Flexibilidade        | Alta: regras de binding no broker                         | Baixa no broker; logica de roteamento fica no producer       |
| Aplicacao aqui       | Um unico exchange `ecommerce` distribui para 5 filas      | Seriam necessarios 5 topicos distintos; producer envia ao    |
|                      | via routing keys (ex.: `pedido.novo`, `pagamento.processar`)| topico certo diretamente                                   |

**Impacto na aplicacao**: O roteamento central do RabbitMQ simplifica o `consumer_pedidos`,
que publica em 3 destinos diferentes usando uma unica conexao com o exchange. No Kafka,
o producer teria que conhecer os 3 topicos e publicar diretamente neles — sem diferenca
funcional, mas a logica de despacho fica no codigo da aplicacao, nao no broker.

---

## 3. Garantias de Entrega

| Aspecto              | RabbitMQ / AMQP                                           | Apache Kafka                                                 |
|----------------------|-----------------------------------------------------------|--------------------------------------------------------------|
| At-most-once         | auto-ack                                                  | enable.auto.commit = true                                    |
| At-least-once        | manual ack (usado nesta aplicacao)                        | manual commit apos processamento (padrao recomendado)        |
| Exactly-once         | Nao suportado nativamente                                 | Suportado com transacoes (Kafka >= 0.11)                     |
| Durabilidade         | durable=True + publisher confirms                         | Replicacao de particoes (replication.factor >= 2)            |

**Impacto na aplicacao**: Para pagamentos, a semântica at-least-once e aceitavel desde que
`consumer_pagamentos` seja idempotente (ex.: verificar se o pedido ja foi pago antes de
processar). O Kafka oferece exactly-once nativo, o que reduziria a complexidade de lidar com
duplicatas em cenarios de falha.

---

## 4. Escalabilidade e Concorrencia

| Aspecto              | RabbitMQ / AMQP                                           | Apache Kafka                                                 |
|----------------------|-----------------------------------------------------------|--------------------------------------------------------------|
| Escalar consumidores | Varios consumers na mesma fila competem pelas mensagens   | Consumers do mesmo grupo dividem as particoes                |
| Limite de escala     | Numero de consumers ativos (qualquer quantidade)          | Numero de particoes do topico                                |
| Throughput           | Dezenas de milhares de msgs/s por no                      | Centenas de milhares a milhoes de msgs/s por cluster         |
| Latencia             | Sub-milissegundo em casos tipicos                         | Tipicamente 2-10 ms (batch interno)                          |

**Impacto na aplicacao**: Para o volume tipico de um e-commerce medio, RabbitMQ e
suficiente. Se o `consumer_pagamentos` se tornar um gargalo, basta iniciar mais instancias
— todas consumirao da mesma fila `queue_pagamentos` de forma automatica. No Kafka, seria
necessario aumentar o numero de particoes do topico antes de adicionar consumers.

---

## 5. Persistencia e Replay

| Aspecto              | RabbitMQ / AMQP                                           | Apache Kafka                                                 |
|----------------------|-----------------------------------------------------------|--------------------------------------------------------------|
| Historico            | Nao: mensagem consumida nao existe mais                   | Sim: todo o historico fica disponivel ate o TTL expirar      |
| Audit trail          | Requer gravacao explicita em banco de dados               | O proprio log e o audit trail                                |
| Re-processar pedidos | Requer armazenamento externo dos payloads                 | Basta resetar o offset do consumer group                     |

**Impacto na aplicacao**: Com RabbitMQ, se `consumer_logistica` ficar fora do ar por horas,
as mensagens ficam na fila aguardando (se a fila for duravel). Ao voltar, processa normalmente.
Porem, se precisarmos *reprocessar* pedidos do dia anterior para corrigir um bug, o RabbitMQ
nao ajuda — precisariamos de um banco de dados auxiliar. O Kafka tornaria esse reprocessamento
trivial.

---

## 6. Operacao e Complexidade

| Aspecto              | RabbitMQ / AMQP                                           | Apache Kafka                                                 |
|----------------------|-----------------------------------------------------------|--------------------------------------------------------------|
| Dependencias         | Erlang + RabbitMQ                                         | JVM + Kafka + ZooKeeper (ou KRaft >= 2.8)                    |
| Interface de admin   | Painel web integrado (porta 15672)                        | Ferramentas CLI; painel via Kafka UI (terceiro)              |
| Protocolo            | AMQP 0-9-1 (padrao aberto)                                | Protocolo binario proprietario do Kafka                      |
| Curva de aprendizado | Baixa a media                                             | Media a alta                                                 |
| Clientes disponiveis | Qualquer linguagem com suporte a AMQP                     | Clientes oficiais para JVM; clientes de comunidade p/ outros |

---

## 7. Analise de Adequacao para Esta Aplicacao

### Por que RabbitMQ e uma boa escolha aqui

1. **Roteamento semantico**: O exchange `ecommerce` distribui mensagens para filas especializadas
   usando routing keys descritivas. Adicionar uma nova fila (ex.: `queue_fraude`) requer apenas
   uma nova binding — sem mudar producers existentes.

2. **Mensagens efemeras**: Pedidos processados nao precisam ser relidos. A semantica
   "consume e descarta" do RabbitMQ e exatamente o que o fluxo exige.

3. **Baixa latencia**: Autorizacao de cartao (consumer_pagamentos) exige resposta rapida.
   RabbitMQ entrega sub-milissegundo, ideal para pipelines interativos.

4. **Simplicidade operacional**: Um servidor RabbitMQ em Docker e suficiente para um
   e-commerce de medio porte, sem necessidade de cluster ZooKeeper/KRaft.

### Quando Kafka seria preferivel

1. **Volume muito alto**: Milhoes de transacoes por dia beneficiam do throughput do Kafka
   e do particionamento horizontal.

2. **Analytics e auditoria**: Se o time de dados precisa reprocessar todos os pedidos do
   trimestre para treinar um modelo de fraude, o log persistente do Kafka e indispensavel.

3. **Event sourcing**: Se o estado do sistema for derivado inteiramente do historico de
   eventos (CQRS/Event Sourcing), o Kafka funciona como fonte de verdade natural.

4. **Multiplos consumidores independentes**: Se `consumer_notificacoes` e um sistema de BI
   precisassem consumir os mesmos eventos de pagamento sem interferencia mutua, o Kafka
   seria superior — no RabbitMQ seria necessario usar fanout exchange ou duplicar filas.

### Resumo

| Criterio                         | Vencedor para esta aplicacao |
|----------------------------------|------------------------------|
| Roteamento flexivel no broker    | RabbitMQ                     |
| Baixa latencia                   | RabbitMQ                     |
| Simplicidade de instalacao       | RabbitMQ                     |
| Replay / reprocessamento         | Kafka                        |
| Throughput massivo               | Kafka                        |
| Multiplos consumers independentes| Kafka                        |
| Exactly-once nativo              | Kafka                        |

**Conclusao**: Para um sistema de pedidos com filas especializadas, fluxo orientado a tarefas
e requisito de baixa latencia, RabbitMQ e a escolha mais adequada. Kafka seria preferivel se
o mesmo pipeline precisasse servir simultaneamente a multiplos sistemas downstream (BI, fraude,
fidelidade) ou se o volume de transacoes exigisse escala horizontal extrema.
