from kafka import KafkaProducer
import json
from datetime import datetime
import time

# Define the Kafka broker and topic
broker = 'my-kafka.gran4u-dev.svc.cluster.local:9092'
topic = 'my-first-topic'

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[broker],
   sasl_mechanism='SCRAM-SHA-256',
     security_protocol='SASL_PLAINTEXT',
     sasl_plain_username='user1',
     sasl_plain_password='B9R3L1I1Uy',
     value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
#producer = KafkaProducer(
#    bootstrap_servers=[broker],
#    value_serializer=lambda v: json.dumps(v).encode('utf-8')
 #)
# Define the message to send

i=0
while True :
    message = {
        'key': 'id',
        'value': i,
        'time': time.time()
    }

# Send the message to the Kafka topic
    producer.send(topic, value=message)
    i+=1
    time.sleep(3)

# Ensure all messages are sent before closing the producer
producer.flush()

print(f"Message sent to topic {topic}")

time.sleep(5)