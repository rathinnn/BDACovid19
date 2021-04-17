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
ACCESS_TOKEN = open(".mapbox_token").read()
#px.set_mapbox_access_token(ACCESS_TOKEN)
req = requests.get('https://api.covid19api.com/live/country/india')

spark = SparkSession.builder.appName("Trial.com").getOrCreate()
json_rdd = spark.sparkContext.parallelize([req.json()])
df = spark.read.json(json_rdd)
df = df.withColumn("Date",df.Date.cast(TimestampType())).withColumn("Lat",df.Lat.cast(DoubleType())).withColumn("Lon",df.Lon.cast(DoubleType()))  
df = df.filter(df.Date>"2021-04-16")
print(df.head())

app = dash.Dash()
app.layout = html.Div(
    [dcc.Graph(id="world-live", animate = True),dcc.Interval(id = 'update',interval = 100000,n_intervals = 0)]
    
)

@app.callback(
    Output("world-live", "figure"),
    [
        Input("update", "n_intervals")
        
    ]
)

def update_graph_world(n):
    s = df.select("Active").rdd.flatMap(lambda x: x).collect()
    '''
    fig = px.scatter_mapbox(
            
            lat= df.select("Lat").rdd.flatMap(lambda x: x).collect(),
            lon= df.select("Lon").rdd.flatMap(lambda x: x).collect(),
            color= s,
            size= s,
            zoom = 4,
            size_max=50,
            
            hover_name= df.select("Province").rdd.flatMap(lambda x: x).collect(),
            hover_data=[s],
            
        )
    '''
    fig = go.Figure(
        go.Scattermapbox(
            lat = df.select("Lat").rdd.flatMap(lambda x: x).collect(),
            lon = df.select("Lon").rdd.flatMap(lambda x: x).collect(),
            mode ='markers',
            marker=go.scattermapbox.Marker(
                size= s,
                sizeref = 5000,
                color = s
            )
        )
    )
    fig.update_layout(
        mapbox=dict(
            accesstoken=ACCESS_TOKEN, 
            
            zoom=4
        )
    )
  
    return fig

if __name__ == '__main__':
    app.run_server()