#Producer

from confluent_kafka import Producer
import json
import sqlite3

me = 'marwa-mahmoud'

# Kafka Producer configuration
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094',
    'client.id': me
}

# Create the Producer instance
producer = Producer(conf)

# SQLite Database Connection
def get_db_connection():
    conn = sqlite3.connect("main.db")
    conn.row_factory = sqlite3.Row
    return conn

# Function to produce a message with image info
def produce_message():
    try:
        topic = me
        con = get_db_connection()
        cur = con.cursor()
        cur.execute("SELECT id, filename FROM image ORDER BY timestamp DESC LIMIT 1")
        row = cur.fetchone()
        con.close()

        if row:
            image_info = {"id": row["id"], "filename": row["filename"]}
            producer.produce(topic, key=str(row["id"]), value=json.dumps(image_info))
            producer.flush()
            print(f"Produced image info to topic '{topic}': {image_info}")
        else:
            print("No images found in the database.")
    except Exception as e:
        print(f"Error producing message: {e}")

# Main execution
if __name__ == "__main__":
    produce_message()