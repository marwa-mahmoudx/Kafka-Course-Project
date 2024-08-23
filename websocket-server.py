#Websocket Server

import asyncio
import websockets
import json
from confluent_kafka import Consumer, KafkaError, KafkaException

me = 'marwa-mahmoud'
groupId = me + '-ws'

# Kafka Consumer Configuration
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'group.id': groupId,
    'enable.auto.commit': True,
    'auto.offset.reset': 'smallest'
}

consumer = Consumer(conf)
topics = [me + '_error', me + '_completed']
consumer.subscribe(topics)

clients = set()

async def register_client(websocket):
    clients.add(websocket)
    try:
        await websocket.wait_closed()
    finally:
        clients.remove(websocket)

async def send_message_to_clients(message):
    if clients:  # Check if there are any connected clients
        await asyncio.wait([client.send(message) for client in clients])

async def consume_kafka():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            else:
                topic = msg.topic()
                key = msg.key().decode('utf-8') if msg.key() else None
                value = msg.value().decode('utf-8') if msg.value() else None
                message = {"topic": topic, "key": key, "value": value}

                # Send message to all connected WebSocket clients
                await send_message_to_clients(json.dumps(message))
    finally:
        consumer.close()

async def main():
    try:
        async with websockets.serve(register_client, "localhost", 8765):
            await consume_kafka()
    except Exception as e:
        print(f"An error occurred in WebSocket server: {e}")

if __name__ == "__main__":
    asyncio.run(main())
