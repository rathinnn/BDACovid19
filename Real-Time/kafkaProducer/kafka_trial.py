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
country = 'india'
try:
    i=0
    while True:
        
        if(i==0):
            i+=1
            h = requests.get('https://corona.lmao.ninja/v2/historical/'+country+'?lastdays=200')
        else:
            h = requests.get('https://corona.lmao.ninja/v2/historical/'+country+'?lastdays=1')
        k = h.json()
        l = k['timeline']
        cases = l['cases']
        deaths = l['deaths']
        recovered = l['recovered']
        jsonlist = []


        for key in cases:
            jsondict = {'Date':str(datetime.datetime.strptime(key, '%m/%d/%y')),'Cases':cases[key],'Deaths':deaths[key],'Recovered':recovered[key]}
            jsonlist.append(jsondict)

        for jso in jsonlist:
            producer.send('test2',jso)
        time.sleep(20)
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass