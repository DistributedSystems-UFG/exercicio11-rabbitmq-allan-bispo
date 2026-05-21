[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/BK9AX0KL)
# RabbitMQ-Example
Example based on Tanenbaum &amp; van Steen (2025)

# Steps to run:

## Ports to open on the firewall (security group on AWS):
```
5671-5672
```

## Install the RabbitMQ broker on a server machine:
You may use the provided script for installation (install_rabbitmq.sh)
```
sudo install_rabbitmq.sh
```
*Note:* Make sure the file is executable (chmod 770 install_rabbitmq.sh)

See installation and configuration details on: https://www.rabbitmq.com/docs/install-debian#apt-quick-start (although the defaults should work just fine for our purposes).

### Once installed, put the broker to run:
```
sudo systemctl start rabbitmq-server
```
### Then create a new RabbitMQ user and password:
```
sudo rabbitmqctl add_user myuser abc123
```
### Now create a vhost in the RabbitMQ server (a vhost is like a container for message queues)?
```
sudo rabbitmqctl add_vhost my_vhost
```
### And give the new user the required permisssions to access the vhost:
```
sudo rabbitmqctl set_permissions -p my_vhost myuser ".*" ".*" ".*"
```

## Finally, install the RabbitMQ python client on the machines where producers and consumers will run:
```
pip install rabbitpy
```

*Note:* Make sure the IP address of the RabbitMQ server is correctly set in const.py

---

# Exemplo Complexo: Sistema de E-commerce

## Arquitetura

```
producer_loja.py   ──► queue_pedidos      ──► consumer_pedidos.py
                                                      │
                          ┌───────────────────────────┼──────────────────────┐
                          ▼                           ▼                      ▼
                   queue_pagamentos          queue_estoque          queue_notificacoes
                          │                           │                      │
               consumer_pagamentos.py    consumer_estoque.py   consumer_notificacoes.py
                          │
              ┌───────────┴──────────────────┐
              ▼                              ▼
      queue_logistica              queue_notificacoes
              │
     consumer_logistica.py

producer_estoque.py ──► queue_estoque ──► consumer_estoque.py
```

Exchange: `ecommerce` (direct)

| Arquivo                    | Papel                                        | Fila consumida      | Filas produzidas                                    |
|----------------------------|----------------------------------------------|---------------------|-----------------------------------------------------|
| `producer_loja.py`         | Produtor 1 – simula pedidos de clientes      | —                   | `queue_pedidos`                                     |
| `producer_estoque.py`      | Produtor 2 – eventos de reabastecimento      | —                   | `queue_estoque`                                     |
| `consumer_pedidos.py`      | Valida pedido e dispara pipeline             | `queue_pedidos`     | `queue_pagamentos`, `queue_estoque`, `queue_notificacoes` |
| `consumer_pagamentos.py`   | Autoriza cartao de credito (85 % aprovacao)  | `queue_pagamentos`  | `queue_notificacoes`, `queue_logistica`             |
| `consumer_estoque.py`      | Atualiza inventario em memoria               | `queue_estoque`     | —                                                   |
| `consumer_notificacoes.py` | Envia email transacional ao cliente          | `queue_notificacoes`| —                                                   |
| `consumer_logistica.py`    | Gera etiqueta e sorteia transportadora       | `queue_logistica`   | —                                                   |

## Como executar

Abra **seis terminais** (ou use tmux/screen). Execute cada comando em um terminal separado:

```bash
# Terminal 1 – Consumidores (iniciar antes dos produtores)
python consumer_pedidos.py

# Terminal 2
python consumer_pagamentos.py

# Terminal 3
python consumer_estoque.py

# Terminal 4
python consumer_notificacoes.py

# Terminal 5
python consumer_logistica.py

# Terminal 6 – Produtores (iniciar depois que os consumidores estiverem prontos)
python producer_loja.py       # publica 5 pedidos
python producer_estoque.py    # publica 4 eventos de estoque
```

## Comparacao RabbitMQ vs Kafka

Ver: [comparacao_rabbitmq_kafka.md](comparacao_rabbitmq_kafka.md)
