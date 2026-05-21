"""
Produtor 2 - Sistema de Gestao de Estoque
Publica eventos de reabastecimento, devolucao e ajuste de inventario
em queue_estoque via routing key estoque.atualizar.
"""
import rabbitpy
import json
import time
import random

from infra import AMQP_URL, RK_ESTOQUE_ATUALIZAR, declare_infra

PRODUTOS = ['P001', 'P002', 'P003', 'P004', 'P005', 'P006']

EVENTOS = [
    ('reabastecimento',    'Carga recebida do fornecedor'),
    ('devolucao',          'Item devolvido pelo cliente'),
    ('ajuste_inventario',  'Correcao apos auditoria fisica'),
]

NUM_EVENTOS = 4

with rabbitpy.Connection(AMQP_URL) as conn:
    with conn.channel() as channel:
        exchange = declare_infra(channel)
        print('[Estoque] Produtor de eventos de estoque iniciado.')
        for i in range(NUM_EVENTOS):
            tipo, motivo = random.choice(EVENTOS)
            evento = {
                'evento_id': f'EVT{i + 1:03d}',
                'produto_id': random.choice(PRODUTOS),
                'tipo': tipo,
                'quantidade': random.randint(10, 100),
                'motivo': motivo,
            }
            msg = rabbitpy.Message(
                channel,
                json.dumps(evento),
                {'content_type': 'application/json'},
            )
            msg.publish(exchange, RK_ESTOQUE_ATUALIZAR)
            print(
                f'[Estoque] Evento publicado: {evento["evento_id"]}'
                f' | Produto: {evento["produto_id"]}'
                f' | Tipo: {evento["tipo"]}'
                f' | Qtd: {evento["quantidade"]}'
            )
            time.sleep(0.8)
        print(f'[Estoque] {NUM_EVENTOS} evento(s) publicado(s).')
