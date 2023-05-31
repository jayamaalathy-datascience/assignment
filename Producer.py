
import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer
class Producer(threading.Thread):
    daemon = True  

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:29092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))        
        with open('locations.json') as f:
            for line in f:
                producer.send('topic22', line.strip())

                
        with open('transactions.json') as f:
            for line in f:
                producer.send('topic23', line.strip())

