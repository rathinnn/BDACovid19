import json
import requests
import time
from kafka import KafkaProducer
from datetime import timezone
import datetime
import calendar
from sendToTopic import sendToCountryTopic

maybeToday = datetime.datetime.utcnow().day

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v,default=json_util.default).encode('utf-8'))
i = 0
countries = ['india','pk','np']

geturl = 'https://api.covid19api.com/live/country/'
try:
    while True:
        maybeTom = datetime.datetime.utcnow().day
        if(maybeToday<maybeTom):
            for country in countries:
                sendToCountryTopic(requests,country,geturl,producer,time)

        
        
        time.sleep(200)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass