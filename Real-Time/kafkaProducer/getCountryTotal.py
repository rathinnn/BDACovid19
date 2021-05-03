import requests
from datetime import datetime
from sendToTopic import sendToCountryTotalTopic
import json
#import requests
import time
from kafka import KafkaProducer



producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
countries = ['india','uk','us','russia']
try:
    i=0
    while True:
        for country in countries:
            if(i==0):
                
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
                jsondict = {'Date':str(datetime.strptime(key, '%m/%d/%y')),'Cases':cases[key],'Deaths':deaths[key],'Recovered':recovered[key]}
                jsonlist.append(jsondict)

            sendToCountryTotalTopic(producer,country,jsonlist)
            #time.sleep(20)
        i+=1
        time.sleep(800)
        
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass