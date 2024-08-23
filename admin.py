#Admin

from confluent_kafka.admin import NewTopic, AdminClient

# Create a KafkaAdminClient instance
conf = {
    'bootstrap.servers': '34.138.205.183:9094,34.138.104.233:9094,34.138.118.154:9094'
}

admin_client = AdminClient(conf)

# Define the topic configuration
me = 'marwa-mahmoud'
num_partitions = 3
replication_factor = 1

# List of topics to be created
topics_to_create = [
    NewTopic(me, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(me + '_error', num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(me + '_completed', num_partitions=num_partitions, replication_factor=replication_factor),
   ]

# Create the new topics
fs = admin_client.create_topics(topics_to_create)

# Check the status of topic creation
for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic '{topic}' created successfully.")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")