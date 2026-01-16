from kafka import KafkaConsumer
import json

# Define the Kafka broker and topic
broker = 'my-kafka.gran4u-dev.svc.cluster.local:9092'
topic = 'partitionned'

# Create a Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='erp',
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_PLAINTEXT',
    sasl_plain_username='user1',
    sasl_plain_password='B9R3L1I1Uy'
)

print(f"Listening to topic {topic}")

# Poll messages from the Kafka topic
for message in consumer:
    print(f"Received message: {message.value}")