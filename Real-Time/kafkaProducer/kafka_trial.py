import json
import requests
import time
from kafka import KafkaProducer
import datetime
datetimetoday = datetime.datetime.utcnow()
#maybeToday = datetoday.day
todaydate = datetime.date.today()
yesterday = todaydate - datetime.timedelta(days=2)
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer = lambda v: json.dumps(v).encode('utf-8'))
i = 0

try:
    while i<=0:
        req = requests.get('https://api.covid19api.com/live/country/india/status/confirmed/date/'+str(yesterday))
        while(req.status_code != 200):
            time.sleep(10)
            req = requests.get('https://api.covid19api.com/live/country/india/status/confirmed/date/'+str(yesterday))
        reqjson = req.json();
        for h in reqjson:
            h['Lat'] = float(h['Lat'])
            h['Lon'] = float(h['Lon'])
            producer.send('test2' , h)
        
        time.sleep(2)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass