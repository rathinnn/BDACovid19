def sendToMapTopic(requests,country,producer,yesterday,time):
    geturl = 'https://api.covid19api.com/live/country/'
    req = requests.get(geturl+country+'/status/confirmed/date/'+str(yesterday))
    while(req.status_code != 200):
        time.sleep(10)
        req = requests.get(geturl+country+'/status/confirmed/date/'+str(yesterday))
    for entry in req.json():
        entry['Lat'] = float(entry['Lat'])
        entry['Lon'] = float(entry['Lon'])
        producer.send(country+'Maptopic' , entry)


def sendToCountryTotalTopic(producer,country,jsonlist):
    for jso in jsonlist:
        producer.send(country+'TotalTopic',jso)

def sendtoLatest(producer,country,jsons):
    for entry in jsons:
        producer.send(country+'Latest' , entry)

