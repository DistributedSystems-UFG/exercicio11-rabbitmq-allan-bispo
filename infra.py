"""
Infraestrutura compartilhada: constantes, nomes de filas/routing keys e
função que declara exchange + filas + bindings (idempotente).
"""
import rabbitpy
from const import RABBITMQ_ADDR

AMQP_URL = f'amqp://myuser:abc123@{RABBITMQ_ADDR}:5672/my_vhost'
EXCHANGE_NAME = 'ecommerce'

# Nomes das filas
Q_PEDIDOS      = 'queue_pedidos'
Q_PAGAMENTOS   = 'queue_pagamentos'
Q_ESTOQUE      = 'queue_estoque'
Q_NOTIFICACOES = 'queue_notificacoes'
Q_LOGISTICA    = 'queue_logistica'

# Routing keys (direct exchange)
RK_PEDIDO_NOVO         = 'pedido.novo'
RK_PAGAMENTO_PROCESSAR = 'pagamento.processar'
RK_ESTOQUE_ATUALIZAR   = 'estoque.atualizar'
RK_NOTIFICACAO_ENVIAR  = 'notificacao.enviar'
RK_LOGISTICA_ENVIAR    = 'logistica.enviar'

_BINDINGS = [
    (Q_PEDIDOS,      RK_PEDIDO_NOVO),
    (Q_PAGAMENTOS,   RK_PAGAMENTO_PROCESSAR),
    (Q_ESTOQUE,      RK_ESTOQUE_ATUALIZAR),
    (Q_NOTIFICACOES, RK_NOTIFICACAO_ENVIAR),
    (Q_LOGISTICA,    RK_LOGISTICA_ENVIAR),
]


def declare_infra(channel):
    """Declara o exchange, as filas e os bindings. Seguro para chamar múltiplas vezes."""
    exchange = rabbitpy.Exchange(channel, EXCHANGE_NAME, exchange_type='direct')
    exchange.declare()
    for queue_name, routing_key in _BINDINGS:
        q = rabbitpy.Queue(channel, queue_name, durable=True, auto_delete=False)
        q.declare()
        q.bind(exchange, routing_key)
    return exchange
