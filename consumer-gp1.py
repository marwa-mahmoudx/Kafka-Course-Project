#Consumer gp1(Object Detection)

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import sys
import json
import sqlite3
import os  # Added to handle file paths
from PIL import Image
#import random  # Added for random object detection logic
import cv2
import numpy as np

me = 'marwa-mahmoud'
groupId = me + 'gp-1'


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


# SQLite Database Connection
def get_db_connection():
    conn = sqlite3.connect("main.db")
    conn.row_factory = sqlite3.Row
    return conn

#def detect_object(id):
    # Simple object detection logic
#    return random.choice(['car', 'house', 'person'])
# Load the image from the filename
    #image_path = os.path.join("images", image_filename)
    #img = Image.open(image_path)


# Face or not detection
def detect_object(image_filename):
    image_path = os.path.join("images", image_filename)
    image = cv2.imread(image_path)

    # Convert image to grayscale
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

    # Load pre-trained Haar cascades for object detection (you can use different cascades for different objects)
    cascade = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")

    # Detect objects in the image
    objects = cascade.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=5, minSize=(30, 30))

    if len(objects) > 0:
        detected_object = 'face'  # Detected a face
    else:
        detected_object = 'not a face'  # unknown object detected

    return detected_object

# Update Detected Object in Database
def update_object_in_db(id, detected_object):
    con = get_db_connection()
    cur = con.cursor()
    cur.execute("UPDATE image SET object = ? WHERE id = ?", (detected_object, id))
    con.commit()
    con.close()

def msg_process(msg):
    try:
        print("Got a new message (gp-1):", msg.value().decode('utf-8'))
        image_info = json.loads(msg.value().decode('utf-8'))
        image_id = image_info['id']
        image_filename = image_info['filename']

        # Perform object detection
        detected_object = detect_object(image_filename)
        print(f"Detected object: {detected_object} for image ID: {image_id}")

        # Update the object in the database
        update_object_in_db(image_id, detected_object)

        # Produce a success message to the completed topic
        producer.produce(me + '_completed', key=str(image_id), value=detected_object)
        producer.flush()

    except Exception as e:
        # Produce an error message to the error topic
        producer.produce(me + '_error', key=str(image_id), value=str(e))
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
