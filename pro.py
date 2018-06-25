from kafka import KafkaProducer
import time
import json

topic = "siva10"
broker_list = "10.0.0.92:9092"
producer = KafkaProducer(bootstrap_servers = [broker_list])
while True: 
    data1 = {"id": 1, "name": "siva1", "sallary": 100, "dept_id": 1}
    data2 = {"id": 2, "name": "siva2", "sallary": 100, "dept_id": 2}
    data3 = {"id": 3, "name": "siva3", "sallary": 700, "dept_id": 1}
    data4 = {"id": 4, "name": "siva4", "sallary": 1000, "dept_id": 2}
    data5 = {"id": 5, "name": "siva5", "sallary": 200, "dept_id": 3}
    for i in range(3): 
        producer.send(topic, json.dumps(data1))
        producer.send(topic, json.dumps(data2))
        producer.send(topic, json.dumps(data3))
        producer.send(topic, json.dumps(data4))
        producer.send(topic, json.dumps(data5))
        time.sleep(5) 
