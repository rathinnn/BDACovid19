import requests
import time
def getInititalMap():
    req = requests.get('https://api.covid19api.com/live/country/india')
    while(req.status_code != 200):
        time.sleep(5)
        req = requests.get('https://api.covid19api.com/live/country/india')
    return req.json()