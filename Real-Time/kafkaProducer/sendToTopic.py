def sendToCountryTopic(requests,country,geturl,producer,time):
    req = requests.get(geturl+country)
    while(req.status_code != 200):
        time.sleep(10)
        req = requests.get(geturl)
    for entry in req.json():
        producer.send(country+'topic' , entry)