import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import requests
import plotly.express as px
import findspark
findspark.init()
from pyspark.sql import SparkSession
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import plotly
import random
import plotly.graph_objs as go
from dash.dependencies import Output, Input
from plots.mapPlot import updateMap
import time

ACCESS_TOKEN = open(".mapbox_token").read()
print(ACCESS_TOKEN)
#px.set_mapbox_access_token(ACCESS_TOKEN)

req = requests.get('https://api.covid19api.com/live/country/india')
while(req.status_code != 200):
    time.sleep(5)
    req = requests.get('https://api.covid19api.com/live/country/india')
    


spark = SparkSession.builder.appName("Trial.com").getOrCreate()
json_rdd = spark.sparkContext.parallelize([req.json()])
df = spark.read.json(json_rdd)
df = df.withColumn("Date",df.Date.cast(TimestampType())).withColumn("Lat",df.Lat.cast(DoubleType())).withColumn("Lon",df.Lon.cast(DoubleType()))  
df = df.filter(df.Date>"2021-04-18")
#print(df.head())

app = dash.Dash()
app.layout = html.Div(
    [dcc.Graph(id="world-live", animate = True),dcc.Interval(id = 'update',interval = 10000,n_intervals = 0)]
    
)

@app.callback(
    Output("world-live", "figure"),
    [
        Input("update", "n_intervals")
        
    ]
)

def update(n):
    return updateMap(go,df,px,n,ACCESS_TOKEN)


if __name__ == '__main__':
    app.run_server()
