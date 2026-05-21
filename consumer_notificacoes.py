"""
Consumidor 4 - Servico de Notificacoes (queue_notificacoes)
Simula o envio de emails transacionais ao cliente em resposta
a eventos do ciclo de vida do pedido.
"""
import rabbitpy
import json
import time
from datetime import datetime

from infra import AMQP_URL, Q_NOTIFICACOES, declare_infra

ASSUNTOS = {
    'pedido_recebido':   'Pedido recebido com sucesso',
    'pagamento_aprovado':'Pagamento confirmado - seu pedido esta a caminho!',
    'pagamento_recusado':'Problema com o pagamento do seu pedido',
    'pedido_enviado':    'Seu pedido foi enviado!',
}


def enviar_email(notif):
    email   = notif.get('cliente_email', 'destinatario@desconhecido.com')
    tipo    = notif.get('tipo', 'generico')
    pid     = notif.get('pedido_id', 'N/A')
    corpo   = notif.get('mensagem', '(sem mensagem)')
    assunto = ASSUNTOS.get(tipo, f'Atualizacao do pedido {pid}')

    time.sleep(0.3)  # Simula chamada a API de email (ex.: SendGrid / SES)

    ts = datetime.now().strftime('%H:%M:%S')
    print(f'[Notificacoes] [{ts}] Para: {email}')
    print(f'[Notificacoes]   Assunto : {assunto}')
    print(f'[Notificacoes]   Conteudo: {corpo}')
    print(f'[Notificacoes]   Status  : ENVIADO')


with rabbitpy.Connection(AMQP_URL) as conn:
    with conn.channel() as channel:
        declare_infra(channel)
        queue = rabbitpy.Queue(channel, Q_NOTIFICACOES)
        print(f'[Notificacoes] Servico de email aguardando mensagens em {Q_NOTIFICACOES} ...')
        for message in queue:
            try:
                notif = json.loads(message.body.decode())
                enviar_email(notif)
                message.ack()
            except Exception as exc:
                print(f'[Notificacoes] ERRO: {exc}')
                message.nack()
