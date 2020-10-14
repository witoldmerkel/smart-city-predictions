from time import sleep
from json import dumps
from kafka import KafkaProducer
import requests


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))

while(1):
    response1 = requests.get("https://api.waqi.info/feed/warsaw/?token=5daa289b09c26a80ee8dfb9f41a19fafd067c10b")
    data1 = response1.json()
    producer.send('numtest', value=data1)

    response2 = requests.get("https://api.waqi.info/feed/ursynow/?token=5daa289b09c26a80ee8dfb9f41a19fafd067c10b")
    data2 = response2.json()
    producer.send('numtest', value=data2)

    sleep(5)