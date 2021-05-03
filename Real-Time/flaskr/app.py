import requests
import pandas as pd
import re
from flask import Flask
from flask import render_template
from dashboard.dashapp import init_dashboard

app = Flask(__name__, instance_relative_config=False)
app = init_dashboard(app)

@app.route("/")
def home():

    return render_template(
        "index.html",
        title="BDA Project Main Page",
        
    )

@app.route('/news')
def updateNews():
    url = "https://vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com/api/news/get-coronavirus-news/0"

    headers = {
        'x-rapidapi-key': "185d06fbe7msh83b44a43c780000p174286jsn1be10d4cfd4a",
        'x-rapidapi-host': "vaccovid-coronavirus-vaccine-and-treatment-tracker.p.rapidapi.com"
        }

    response = requests.request("GET", url, headers=headers)
    dictionary = response.json()
    news = dictionary['news']
    news.reverse()
    title = []
    link = []
    pub = []
    content = []
    reference = []
    for i in range(len(news)):
        title.append(news[i]['title'])
        link.append(news[i]['link'])
        pub.append(news[i]['pubDate'])
        preprocess = re.sub(r"\[.*?\]", "", news[i]['content'])
        preprocess = re.sub(r"\<.*?\>", "", preprocess)
        content.append(preprocess)
        reference.append(news[i]['reference'])
    return render_template('news.html', content = content, title = title, link = link, length = len(news))

if __name__ == '__main__':
    app.run()
