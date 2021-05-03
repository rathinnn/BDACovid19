import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.express as px
#import findspark
#findspark.init()
from pyspark.sql import SparkSession

import plotly
import random
import plotly.graph_objs as go
from dash.dependencies import Output, Input
from plots.mapPlot import updateMap
from getData import getInititalMap
from SparkJobs import getinitialMapDF, startMapStreamingDF, IndiaMapDF, USMapDF, UKMapDF, RussiaMapDF

def init_dashboard(server):
    ACCESS_TOKEN = open(".mapbox_token").read()
    #print(ACCESS_TOKEN)
    #px.set_mapbox_access_token(ACCESS_TOKEN)


    #map_json = getInititalMap()    


    spark = SparkSession.builder.appName("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    #df = getinitialMapDF(spark,map_json)
    
    indiamapDf = IndiaMapDF(spark)
    curDf = indiamapDf
    ukmapDf = UKMapDF(spark)
    usmapDf = USMapDF(spark)
    russiamapDf = RussiaMapDF(spark)
    allmaps ={}
    allmaps["India"] = indiamapDf
    allmaps["USA"] = usmapDf
    allmaps["UK"] = ukmapDf
    allmaps["Russia"] = russiamapDf
    #names2 = ["India","USA","UK"]
    app = dash.Dash(server = server,routes_pathname_prefix="/dashboard/")
    countries = ["India","USA","UK","Russia"]
    
    app.layout = html.Div(
        children=[
            html.Div(
                children=[
                    
                    html.H1(
                        children="Covid Dashboard", className="dash-head1"
                    ),
                    html.P(
                        children="Developed For 19AIE214 Big Data Analytics By Group 8",
                        className="dash-desc",
                    ),
                ],
                className="dashheader",
            ),
            html.Div(
                children=[
                    html.Div(
                        children=[
                            html.Div(children="Country", className="menu-title"),
                            dcc.Dropdown(
                                id="country",
                                options=[
                                    {"label": country, "value": country}
                                    for country in countries
                                ],
                                value="India",
                                clearable=False,
                                className="countrydrop",
                            ),
                        ]
                    ),

                ],
                className="menu",
            ),
            html.Div(
                [
                    html.Div(
                        children= dcc.Graph(
                            id="world-live", 
                            #animate = True
                        ), 
                        
                    ),

                    html.Div(
                        children=dcc.Graph(
                            id="volume-chart",
                            config={"displayModeBar": False},
                        ),
                        className="card",
                    ),

                    dcc.Interval(id = 'update',interval = 20000,n_intervals = 0),
                ],
                
                className="wrapper",
            ),
        ]
    )
   

    @app.callback(Output("world-live", "figure"),[Input("update", "n_intervals"),Input("country", "value")])
    def update(n_intervals,country):
        print(country)
        curdf = allmaps[country]
        #curdf.show()
        return updateMap(go,curdf,px,ACCESS_TOKEN)

    return app.server

    