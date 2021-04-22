import json
import requests
import time
from kafka import KafkaProducer
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
i = 0
try:
    while True:
        req = requests.get('https://api.covid19api.com/live/country/india')
        while(req.status_code != 200):
            time.sleep(10)
            req = requests.get('https://api.covid19api.com/live/country/india')
        
        producer.send('TutorialTopic' , req.json())
        s = '''++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
        =================================================================================
        ===================================================================
        sssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss
        ssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss'''+str(i)+'/n'
        producer.send('TutorialTopic' , s)
        time.sleep(10)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass