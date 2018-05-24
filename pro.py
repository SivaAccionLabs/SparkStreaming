from kafka import KafkaProducer
import time
import json

topic = "hbase"
broker_list = "10.0.0.70:9092"
producer = KafkaProducer(bootstrap_servers = [broker_list])
while True:  
    data = {"id": 5, "name": "siva"}
    producer.send(topic, json.dumps(data))
    time.sleep(5) 
