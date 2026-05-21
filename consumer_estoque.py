"""
Consumidor 3 - Gerenciador de Estoque (queue_estoque)
Mantém um inventário em memória e processa dois tipos de evento:
  - reserva/saida : subtrai do estoque e alerta se ficar abaixo do minimo
  - reabastecimento / devolucao / ajuste_inventario : soma ao estoque
"""
import rabbitpy
import json
import time

from infra import AMQP_URL, Q_ESTOQUE, declare_infra

ESTOQUE_MINIMO = 5

# Inventario em memória (simulando banco de dados)
inventario = {
    'P001': 15,
    'P002': 50,
    'P003': 30,
    'P004': 8,
    'P005': 25,
    'P006': 12,
}

OPERACOES_SAIDA   = {'reserva', 'saida', 'venda'}
OPERACOES_ENTRADA = {'reabastecimento', 'devolucao', 'ajuste_inventario'}


def atualizar_estoque(evento):
    produto_id = evento.get('produto_id', '???')
    quantidade  = int(evento.get('quantidade', 0))
    operacao    = evento.get('tipo', 'ajuste')
    pedido_ref  = evento.get('pedido_id', '')
    motivo      = evento.get('motivo', operacao)

    atual = inventario.get(produto_id, 0)

    if operacao in OPERACOES_SAIDA:
        if atual < quantidade:
            print(
                f'[Estoque] ALERTA DE RUPTURA: {produto_id} | '
                f'disponivel={atual}, solicitado={quantidade} | pedido={pedido_ref}'
            )
            inventario[produto_id] = 0
            return
        inventario[produto_id] = atual - quantidade
        sufixo = f'pedido {pedido_ref}' if pedido_ref else motivo
        print(f'[Estoque] {produto_id}: {atual} -> {inventario[produto_id]} (reserva para {sufixo})')

    elif operacao in OPERACOES_ENTRADA:
        inventario[produto_id] = atual + quantidade
        print(f'[Estoque] {produto_id}: {atual} -> {inventario[produto_id]} ({operacao}, +{quantidade})')
    else:
        print(f'[Estoque] Operacao desconhecida "{operacao}" para {produto_id}. Ignorado.')
        return

    novo = inventario[produto_id]
    if novo <= ESTOQUE_MINIMO:
        print(f'[Estoque] *** REPOSICAO NECESSARIA: {produto_id} com apenas {novo} unidade(s)! ***')

    time.sleep(0.2)


with rabbitpy.Connection(AMQP_URL) as conn:
    with conn.channel() as channel:
        declare_infra(channel)
        queue = rabbitpy.Queue(channel, Q_ESTOQUE)
        print(f'[Estoque] Gerenciador iniciado | inventario inicial: {inventario}')
        print(f'[Estoque] Aguardando eventos em {Q_ESTOQUE} ...')
        for message in queue:
            try:
                evento = json.loads(message.body.decode())
                atualizar_estoque(evento)
                message.ack()
            except Exception as exc:
                print(f'[Estoque] ERRO: {exc}')
                message.nack()
