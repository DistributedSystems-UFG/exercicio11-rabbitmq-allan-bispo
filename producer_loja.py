"""
Produtor 1 - Loja Online
Simula clientes fazendo pedidos. Publica em queue_pedidos via routing key pedido.novo.
"""
import rabbitpy
import json
import time
import random
import uuid

from infra import AMQP_URL, RK_PEDIDO_NOVO, declare_infra

PRODUTOS = [
    {'id': 'P001', 'nome': 'Notebook Dell Inspiron', 'preco': 3500.00},
    {'id': 'P002', 'nome': 'Mouse Logitech MX Master', 'preco': 85.00},
    {'id': 'P003', 'nome': 'Teclado Mecanico Keychron', 'preco': 250.00},
    {'id': 'P004', 'nome': 'Monitor LG 24"',            'preco': 1200.00},
    {'id': 'P005', 'nome': 'Headset HyperX Cloud',      'preco': 350.00},
    {'id': 'P006', 'nome': 'Webcam Logitech C920',      'preco': 420.00},
]

CLIENTES = [
    {'id': 'C001', 'nome': 'Maria Silva',   'email': 'maria@email.com'},
    {'id': 'C002', 'nome': 'Joao Santos',   'email': 'joao@email.com'},
    {'id': 'C003', 'nome': 'Ana Oliveira',  'email': 'ana@email.com'},
    {'id': 'C004', 'nome': 'Carlos Pereira','email': 'carlos@email.com'},
]


def gerar_pedido():
    cliente = random.choice(CLIENTES)
    produtos_escolhidos = random.sample(PRODUTOS, k=random.randint(1, 3))
    itens = [
        {
            'produto_id': p['id'],
            'nome': p['nome'],
            'preco_unitario': p['preco'],
            'quantidade': random.randint(1, 2),
        }
        for p in produtos_escolhidos
    ]
    total = sum(i['preco_unitario'] * i['quantidade'] for i in itens)
    return {
        'pedido_id': str(uuid.uuid4())[:8].upper(),
        'cliente': cliente,
        'itens': itens,
        'total': round(total, 2),
        'status': 'aguardando_pagamento',
    }


NUM_PEDIDOS = 5

with rabbitpy.Connection(AMQP_URL) as conn:
    with conn.channel() as channel:
        exchange = declare_infra(channel)
        print('[Loja] Produtor iniciado. Publicando pedidos...')
        for _ in range(NUM_PEDIDOS):
            pedido = gerar_pedido()
            msg = rabbitpy.Message(
                channel,
                json.dumps(pedido),
                {'content_type': 'application/json'},
            )
            msg.publish(exchange, RK_PEDIDO_NOVO)
            print(
                f'[Loja] Pedido publicado: {pedido["pedido_id"]}'
                f' | Cliente: {pedido["cliente"]["nome"]}'
                f' | Total: R${pedido["total"]:.2f}'
                f' | Itens: {len(pedido["itens"])}'
            )
            time.sleep(1.5)
        print(f'[Loja] {NUM_PEDIDOS} pedido(s) publicado(s).')
