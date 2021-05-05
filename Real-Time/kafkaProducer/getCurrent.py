import json
#import requests
import time
from kafka import KafkaProducer
from datetime import timezone
import requests
import datetime
from sendToTopic import sendtoLatest

producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
countries = ['India','USA','UK','Russia']
countrycode = {
    'India' : 'ind',
    'USA' : 'usa',
    'UK' : 'gbr',
    'Russia' : 'rus'
}

url = "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/npm-covid-data/country-report-iso-based/"

headers = {
    'x-rapidapi-key': "bd2ff67577msh2a3e376cdbfe879p1511fcjsn7a8e8b6ff090",
    'x-rapidapi-host': "vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com"
    }


try:
    i=2
    while True:
        
        i+=1
        
        for country in countries:
            req = requests.get(url+country+'/'+countrycode[country],headers=headers)
            #print(req.json())
            while(req.status_code != 200):
                time.sleep(10)
                req = requests.get(url+country+'/'+countrycode[country],headers=headers)
            sendtoLatest(producer,country,req.json())
            time.sleep(5)
        print('Ready Latest')

        if(i<20):
            time.sleep(10)
        else:
            time.sleep(600)
        
except KeyboardInterrupt:

    print("Press Ctrl-C to terminate while statement")

    pass