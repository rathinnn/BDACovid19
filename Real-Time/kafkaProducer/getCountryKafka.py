import json
#import requests
import time
from kafka import KafkaProducer
from datetime import timezone
import requests
import datetime
from sendToTopic import sendToMapTopic

datetimetoday = datetime.datetime.utcnow()
maybeToday = datetimetoday.day
todaydate = datetime.date.today()
yesterday = todaydate - datetime.timedelta(days=1)
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
i = 0
countries = ['india','us','uk','russia']

#geturl = 'https://api.covid19api.com/live/country/'

try:
    while True:
        maybeTom = datetime.datetime.utcnow().day
        if(maybeToday != maybeTom or i==0):
            maybeToday = maybeTom
            todaydate = datetime.date.today()
            yesterday = todaydate - datetime.timedelta(days=1)
            for country in countries:
                sendToMapTopic(requests,country,producer,yesterday,time)
                time.sleep(5)
                #maybeToday = maybeTom
            print('Ready')
        else:
            print('Still Not ready')
        time.sleep(20)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass