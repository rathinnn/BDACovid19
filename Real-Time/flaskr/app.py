import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.express as px
import findspark
findspark.init()
from pyspark.sql import SparkSession

import plotly
import random
import plotly.graph_objs as go
from dash.dependencies import Output, Input
from plots.mapPlot import updateMap
from getData import getInititalMap
from SparkJobs import getinitialMapDF

ACCESS_TOKEN = open(".mapbox_token").read()
#print(ACCESS_TOKEN)
#px.set_mapbox_access_token(ACCESS_TOKEN)


map_json = getInititalMap()    


spark = SparkSession.builder.appName("local").getOrCreate()

df = getinitialMapDF(spark,map_json)
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
