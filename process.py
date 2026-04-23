import sys
from kafka import KafkaConsumer, KafkaProducer
from const import BROKER_ADDR, BROKER_PORT

def validate_args():
    if len(sys.argv) < 3:
        print('Uso: python3 processor.py <topico_entrada> <topico_saida>')
        sys.exit(1)

def build_broker_url():
    return f"{BROKER_ADDR}:{BROKER_PORT}"

def extract_message_number(text):
    partes = text.split('My ')
    numero = partes[1].split('st ')[0]
    return int(numero)

def transform_message(original):
    num = extract_message_number(original)
    return f"Minha {num}º transformada do topico 1"

def create_consumer(broker, topic):
    return KafkaConsumer(
        topic,
        bootstrap_servers=[broker],
        auto_offset_reset='earliest',
        group_id='processor-group',
    )

def create_producer(broker):
    return KafkaProducer(bootstrap_servers=[broker])

def run():
    validate_args()

    topic_in  = sys.argv[1]
    topic_out = sys.argv[2]
    broker    = build_broker_url()

    consumer = create_consumer(broker, topic_in)
    producer = create_producer(broker)

    print(f'[processor] {topic_in} --> {topic_out}')
    print('[processor] aguardando mensagens...\n')

    try:
        for mensagem in consumer:
            texto_original  = mensagem.value.decode()
            texto_processado = transform_message(texto_original)

            print(f'Recebido:   {texto_original}')
            print(f'Publicando: {texto_processado}\n')

            producer.send(topic_out, value=texto_processado.encode())
    finally:
        producer.flush()

if __name__ == '__main__':
    run()
