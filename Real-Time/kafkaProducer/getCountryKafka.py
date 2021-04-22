import json
import requests
import time
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
i = 0
country = 'india'
geturl = 'https://api.covid19api.com/live/country/'+country
try:
    while True:
        req = requests.get(geturl)
        while(req.status_code != 200):
            time.sleep(10)
            req = requests.get(geturl)
        
        producer.send('TutorialTopic' , req.json())

        time.sleep(200)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass