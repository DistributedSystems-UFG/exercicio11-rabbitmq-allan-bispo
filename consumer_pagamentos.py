"""
Consumidor 2 - Gateway de Pagamento (queue_pagamentos)
Simula a autorizacao do cartao de credito.
- Pagamento aprovado  -> publica em queue_notificacoes + queue_logistica
- Pagamento recusado  -> publica apenas em queue_notificacoes
"""
import rabbitpy
import json
import time
import random

from infra import (
    AMQP_URL, Q_PAGAMENTOS,
    RK_NOTIFICACAO_ENVIAR, RK_LOGISTICA_ENVIAR,
    declare_infra,
)

TAXA_APROVACAO = 0.85  # 85 % dos pagamentos sao aprovados


def _publicar(channel, exchange, routing_key, payload):
    msg = rabbitpy.Message(channel, json.dumps(payload), {'content_type': 'application/json'})
    msg.publish(exchange, routing_key)


def processar_pagamento(pagamento, exchange, channel):
    pid = pagamento['pedido_id']
    valor = pagamento['valor']
    print(f'[Pagamentos] Autorizando R${valor:.2f} para pedido {pid} ...')

    time.sleep(1.0)  # Simula latencia da operadora de cartao

    aprovado = random.random() < TAXA_APROVACAO

    if aprovado:
        codigo_auth = f'AUTH{random.randint(100000, 999999)}'
        print(f'[Pagamentos] APROVADO {pid} | Codigo: {codigo_auth}')
        _publicar(channel, exchange, RK_NOTIFICACAO_ENVIAR, {
            'pedido_id': pid,
            'cliente_email': pagamento['cliente']['email'],
            'tipo': 'pagamento_aprovado',
            'mensagem': (
                f'Pagamento de R${valor:.2f} aprovado (Auth: {codigo_auth}). '
                f'Seu pedido {pid} sera enviado em breve.'
            ),
        })
        _publicar(channel, exchange, RK_LOGISTICA_ENVIAR, {
            'pedido_id': pid,
            'cliente': pagamento['cliente'],
            'valor': valor,
            'prioridade': 'normal',
        })
        print(f'[Pagamentos] Solicitacao de envio criada para {pid}')
    else:
        motivo = random.choice(['saldo insuficiente', 'cartao expirado', 'limite excedido'])
        print(f'[Pagamentos] RECUSADO {pid} | Motivo: {motivo}')
        _publicar(channel, exchange, RK_NOTIFICACAO_ENVIAR, {
            'pedido_id': pid,
            'cliente_email': pagamento['cliente']['email'],
            'tipo': 'pagamento_recusado',
            'mensagem': (
                f'Nao foi possivel processar o pagamento do pedido {pid} '
                f'({motivo}). Por favor, verifique seus dados e tente novamente.'
            ),
        })


with rabbitpy.Connection(AMQP_URL) as conn:
    with conn.channel() as channel:
        exchange = declare_infra(channel)
        queue = rabbitpy.Queue(channel, Q_PAGAMENTOS)
        print(f'[Pagamentos] Gateway aguardando mensagens em {Q_PAGAMENTOS} ...')
        for message in queue:
            try:
                pagamento = json.loads(message.body.decode())
                processar_pagamento(pagamento, exchange, channel)
                message.ack()
            except Exception as exc:
                print(f'[Pagamentos] ERRO: {exc}')
                message.nack()
