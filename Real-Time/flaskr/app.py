import numpy as np
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
    npurls = np.load('urls.npy')
    urls = npurls.tolist()
    nplink = np.load('link.npy')
    link = nplink.tolist()
    nptitle = np.load('title.npy')
    title = nptitle.tolist()
    npcontent = np.load('content.npy')
    content = npcontent.tolist()
    return render_template('news.html', urls = urls, content = content, title = title, link = link, length = len(title))

if __name__ == '__main__':
    app.run()
