import requests
from datetime import datetime
from sendToTopic import sendToCountryTotalTopic
import json
#import requests
import time
from kafka import KafkaProducer



producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
countries = ['india']#,'pk','np']
for country in countries:

    h = requests.get('https://corona.lmao.ninja/v2/historical/'+country+'?lastdays=200')
    k = h.json()
    l = k['timeline']
    cases = l['cases']
    deaths = l['deaths']
    recovered = l['recovered']
    jsonlist = []


    for key in cases:
        jsondict = {'Date':str(datetime.strptime(key, '%m/%d/%y')),'Cases':cases[key],'Deaths':deaths[key],'Recovered':recovered[key]}
        jsonlist.append(jsondict)



    sendToCountryTotalTopic(producer,country,jsonlist)
    time.sleep(20)
time.sleep(800)
