from confluent_kafka import Consumer, KafkaError, KafkaException
import sys

me = 'marwa-mahmoud'
groupId = me + 'gp-s'

conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'group.id': groupId,
    'enable.auto.commit': True,
    'auto.offset.reset': 'smallest'
}

consumer = Consumer(conf)
topics = [me]
consumer.subscribe(topics)

def msg_process(msg):
    print("Got a new message (gp-s):", msg.value().decode('utf-8'))

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # END OF PARTITION EVENT
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            else:
                raise KafkaException(msg.error())
        else:
            msg_process(msg)
            # Optionally commit the message synchronously (if auto-commit is off)
            # consumer.commit(asynchronous=False)
finally:
    consumer.close()