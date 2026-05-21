"""
Consumidor 5 - Sistema de Logistica (queue_logistica)
Gera a etiqueta de envio, sorteia a transportadora e calcula o prazo.
So recebe mensagens quando o pagamento foi aprovado.
"""
import rabbitpy
import json
import time
import random
import string

from infra import AMQP_URL, Q_LOGISTICA, declare_infra

TRANSPORTADORAS = [
    {'nome': 'Correios PAC',    'prazo_min': 5,  'prazo_max': 12},
    {'nome': 'Correios SEDEX',  'prazo_min': 1,  'prazo_max': 3},
    {'nome': 'JadLog',          'prazo_min': 3,  'prazo_max': 7},
    {'nome': 'Total Express',   'prazo_min': 2,  'prazo_max': 5},
]


def gerar_rastreio():
    letras   = ''.join(random.choices(string.ascii_uppercase, k=2))
    numeros  = ''.join(random.choices(string.digits, k=9))
    return f'{letras}{numeros}BR'


def processar_envio(envio):
    pid        = envio['pedido_id']
    cliente    = envio['cliente']
    valor      = envio.get('valor', 0)
    prioridade = envio.get('prioridade', 'normal')

    time.sleep(0.8)  # Simula integracao com API da transportadora

    transportadora = random.choice(TRANSPORTADORAS)
    rastreio = gerar_rastreio()
    prazo    = random.randint(transportadora['prazo_min'], transportadora['prazo_max'])

    print(f'[Logistica] Etiqueta gerada para pedido {pid}')
    print(f'[Logistica]   Destinatario  : {cliente["nome"]} ({cliente["email"]})')
    print(f'[Logistica]   Transportadora: {transportadora["nome"]}')
    print(f'[Logistica]   Rastreio       : {rastreio}')
    print(f'[Logistica]   Prazo estimado : {prazo} dia(s) util(eis)')
    print(f'[Logistica]   Valor do pedido: R${valor:.2f} | Prioridade: {prioridade}')


with rabbitpy.Connection(AMQP_URL) as conn:
    with conn.channel() as channel:
        declare_infra(channel)
        queue = rabbitpy.Queue(channel, Q_LOGISTICA)
        print(f'[Logistica] Sistema de logistica aguardando em {Q_LOGISTICA} ...')
        for message in queue:
            try:
                envio = json.loads(message.body.decode())
                processar_envio(envio)
                message.ack()
            except Exception as exc:
                print(f'[Logistica] ERRO: {exc}')
                message.nack()
