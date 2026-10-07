from confluent_kafka import Consumer, KafkaError
import json
import os
from time import sleep
import logging
import requests


token=os.environ['Token_telegram']
id=os.environ['Id_telegram']

def enviar_mensagem(texto):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    r=requests.post(url, data={'chat_id': id, 'text': texto})
    r.raise_for_status()

### Consumer
c = Consumer({
    'bootstrap.servers': 'kafka1:19091,kafka2:19092,kafka3:19093',
    'group.id': 'notificador-group',
    'client.id': 'client-1',
    'enable.auto.commit': True,
    'session.timeout.ms': 6000,
    'default.topic.config': {'auto.offset.reset': 'smallest'}
})

c.subscribe(['notificacao'])

try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            data = json.loads(msg.value())
            texto = f"O arquivo {data['filename']} foi {data['operation']}."
            logging.warning(f"SENDING {texto}")
            try:
                enviar_mensagem(texto)
                logging.warning("Mensagem enviada")
            except Exception as e:
                logging.error(f"Erro ao tentar enviar a mensagem: {e}")
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            logging.warning('End of partition reached {0}/{1}'
                  .format(msg.topic(), msg.partition()))
        else:
            logging.error('Error occured: {0}'.format(msg.error().str()))

except KeyboardInterrupt:
    pass
finally:
    c.close()