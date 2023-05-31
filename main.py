
import threading
import logging
import time
import json
from kafka import KafkaConsumer, KafkaProducer
from Consumer import Consumer
from Producer import Producer
def main():
    threads = [
        Producer()
        Consumer()    
        ]  
    for t in threads:
        t.start()  
        time.sleep(10000)

if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:' +
               '%(levelname)s:%(process)d:%(message)s',
        level=logging.INFO
    )
    main()