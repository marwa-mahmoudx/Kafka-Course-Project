#Consumer gp2 (Black and white Transformation)

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import sys
import sqlite3
from PIL import Image
import os
import json

me = 'marwa-mahmoud'
groupId = me + 'gp-2'

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'group.id': groupId,
    'enable.auto.commit': True,
    'auto.offset.reset': 'smallest'
}

consumer = Consumer(conf)
topics = [me]
consumer.subscribe(topics)

producer_conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'client.id': me
}

producer = Producer(producer_conf)

def get_db_connection():
    conn = sqlite3.connect("main.db")
    conn.row_factory = sqlite3.Row
    return conn

def convert_image_to_bw(image_path):
    image = Image.open(image_path).convert('L')  # Convert image to black and white
    image.save(image_path)  # Overwrite the original image

def msg_process(msg):
    try:
        print("Got a new message (gp-2):", msg.value().decode('utf-8'))
        image_info = json.loads(msg.value().decode('utf-8'))
        image_id = image_info['id']

        # Get the image filename from the database
        con = get_db_connection()
        cur = con.cursor()
        cur.execute("SELECT filename FROM image WHERE id = ?", (image_id,))
        row = cur.fetchone()
        con.close()

        if row:
            image_filename = row['filename']
            image_path = os.path.join("images", image_filename)

            # Convert the image to black and white
            convert_image_to_bw(image_path)
            print(f"Converted {image_filename} to black and white.")

            # Produce a success message for the image conversion
            producer.produce(me + '_completed', key=image_id, value=image_filename)
            producer.flush()

    except Exception as e:
        # Produce an error message to the error topic
        producer.produce(me + '_error', key=image_id, value=str(e))
        producer.flush()
        print(f"Error processing message: {e}")

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                 (msg.topic(), msg.partition(), msg.offset()))
            else:
                raise KafkaException(msg.error())
        else:
            msg_process(msg)
finally:
    consumer.close()
