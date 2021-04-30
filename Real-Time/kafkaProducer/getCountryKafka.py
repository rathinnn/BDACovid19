import json
#import requests
import time
from kafka import KafkaProducer
from datetime import timezone
import datetime
import calendar
from sendToTopic import sendToMapTopic

datetimetoday = datetime.datetime.utcnow()
maybeToday = datetimetoday.day
todaydate = datetime.date.today()
yesterday = todaydate - datetime.timedelta(days=1)
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
i = 0
countries = ['india','pk','np']

#geturl = 'https://api.covid19api.com/live/country/'
try:
    while True:
        maybeTom = datetime.datetime.utcnow().day
        if(maybeToday != maybeTom):
        for country in countries:
            sendToMapTopic(requests,country,producer,yesterday,time)

            #maybeToday = maybeTom
        
        time.sleep(800)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass