import json
import requests
import time
from kafka import KafkaProducer
import datetime
#datetimetoday = datetime.datetime.utcnow()
#maybeToday = datetoday.day
todaydate = datetime.date.today()
yesterday = todaydate - datetime.timedelta(days=1)
producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer = lambda v: json.dumps(v).encode('utf-8'))
i = 0
datetimetoday = datetime.datetime.utcnow()
maybeToday = datetimetoday.day
todaydate = datetime.date.today()

try:
    while True:
        maybeTom = datetime.datetime.utcnow().day
        if(maybeToday != maybeTom or i==0):
            todaydate = datetime.date.today()
            yesterday = todaydate - datetime.timedelta(days=1)
            req = requests.get('https://api.covid19api.com/live/country/us/status/confirmed/date/'+str(yesterday))
            while(req.status_code != 200):
                time.sleep(10)
                req = requests.get('https://api.covid19api.com/live/country/us/status/confirmed/date/'+str(yesterday))
            reqjson = req.json();
            for h in reqjson:
                h['Lat'] = float(h['Lat'])
                h['Lon'] = float(h['Lon'])
                producer.send('test2' , h)
            print('Ready')
        else:
            print('Still Not ready')
        time.sleep(200)
        i += 1
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass