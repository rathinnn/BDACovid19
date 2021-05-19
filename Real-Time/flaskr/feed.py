import time
from selenium import webdriver
import requests
import re
import numpy as np
wd = webdriver.Chrome()
url = "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/news/get-coronavirus-news/0"

headers = {
    'x-rapidapi-key': "185d06fbe7msh83b44a43c780000p174286jsn1be10d4cfd4a",
    'x-rapidapi-host': "vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com"
    }

response = requests.request("GET", url, headers=headers)
dictionary = response.json()
news = dictionary['news']
title = []
link = []
pub = []
content = []
reference = []
for i in range(len(news)):
    if(not news[i]['title'] in title):
        title.append(news[i]['title'])
        link.append(news[i]['link'])
        pub.append(news[i]['pubDate'])
        preprocess = re.sub(r"\[.*?\]", "", news[i]['content'])
        preprocess = re.sub(r"\<.*?\>", "", preprocess)
        content.append(preprocess)
        reference.append(news[i]['reference'])

def getUrl(topic):
    searchImage = "https://www.google.com/search?safe=off&site=&tbm=isch&source=hp&q={q}&oq={q}&gs_l=img"
    wd.get(searchImage.format(q=topic))
    thumbnails = wd.find_elements_by_css_selector("img.rg_i")
    length = len(thumbnails)
    url=""
    for img in thumbnails[0:length]:
        try:
            img.click()
            time.sleep(3)
        except Exception:
            continue
        images = wd.find_elements_by_css_selector("img.n3VNCb")
        for image in images:
            if image.get_attribute("src") and "http" in image.get_attribute("src"):
                url = image.get_attribute("src")
                if(url!=''):
                    break
        break
    return url
urls=[]
for i in range(len(title)):
    urls.append(getUrl(title[i]))
wd.quit()
npurls = np.array(urls)
npcontent = np.array(content)
nptitle = np.array(title)
nplink = np.array(link)
np.save('urls', npurls)
np.save('content', npcontent)
np.save('title', nptitle)
np.save('link', nplink)
