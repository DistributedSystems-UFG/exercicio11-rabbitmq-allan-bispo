"""
Consumidor 1 - Processador de Pedidos (queue_pedidos)
Valida o pedido recebido e dispara tres acoes paralelas:
  1. Solicita processamento de pagamento (queue_pagamentos)
  2. Solicita reserva de estoque por item (queue_estoque)
  3. Notifica o cliente sobre o recebimento do pedido (queue_notificacoes)
"""
import rabbitpy
import json
import time

from infra import (
    AMQP_URL, Q_PEDIDOS,
    RK_PAGAMENTO_PROCESSAR, RK_ESTOQUE_ATUALIZAR, RK_NOTIFICACAO_ENVIAR,
    declare_infra,
)


def _publicar(channel, exchange, routing_key, payload):
    msg = rabbitpy.Message(channel, json.dumps(payload), {'content_type': 'application/json'})
    msg.publish(exchange, routing_key)


def processar_pedido(pedido, exchange, channel):
    pid = pedido['pedido_id']
    print(f'[Pedidos] Recebido: {pid} | Cliente: {pedido["cliente"]["nome"]} | R${pedido["total"]:.2f}')

    if pedido['total'] <= 0 or not pedido.get('itens'):
        print(f'[Pedidos] REJEITADO {pid}: pedido invalido.')
        return

    time.sleep(0.4)  # Simula validacao de regras de negocio

    # 1. Solicita pagamento
    _publicar(channel, exchange, RK_PAGAMENTO_PROCESSAR, {
        'pedido_id': pid,
        'cliente': pedido['cliente'],
        'valor': pedido['total'],
        'metodo_pagamento': 'cartao_credito',
    })
    print(f'[Pedidos] Pagamento solicitado para {pid}')

    # 2. Reserva estoque de cada item
    for item in pedido['itens']:
        _publicar(channel, exchange, RK_ESTOQUE_ATUALIZAR, {
            'pedido_id': pid,
            'produto_id': item['produto_id'],
            'quantidade': item['quantidade'],
            'tipo': 'reserva',
            'motivo': f'Reserva para pedido {pid}',
        })
    print(f'[Pedidos] Reserva de estoque solicitada para {len(pedido["itens"])} item(ns)')

    # 3. Notifica cliente
    _publicar(channel, exchange, RK_NOTIFICACAO_ENVIAR, {
        'pedido_id': pid,
        'cliente_email': pedido['cliente']['email'],
        'tipo': 'pedido_recebido',
        'mensagem': (
            f'Seu pedido {pid} foi recebido com sucesso e esta sendo processado. '
            f'Total: R${pedido["total"]:.2f}'
        ),
    })
    print(f'[Pedidos] Notificacao enviada para {pedido["cliente"]["email"]}')


with rabbitpy.Connection(AMQP_URL) as conn:
    with conn.channel() as channel:
        exchange = declare_infra(channel)
        queue = rabbitpy.Queue(channel, Q_PEDIDOS)
        print(f'[Pedidos] Consumidor aguardando mensagens em {Q_PEDIDOS} ...')
        for message in queue:
            try:
                pedido = json.loads(message.body.decode())
                processar_pedido(pedido, exchange, channel)
                message.ack()
            except Exception as exc:
                print(f'[Pedidos] ERRO: {exc}')
                message.nack()
