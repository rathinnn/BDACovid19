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
from plots.graphPlot import updateCases, updateDeaths, updateRecovered
from getData import getInititalMap
from SparkJobs import getinitialMapDF, startMapStreamingDF, IndiaMapDF, USMapDF, UKMapDF, RussiaMapDF
from SparkJobs import IndiaTotalDF, UKTotalDF, USTotalDF, RussiaTotalDF

def init_dashboard(server):
    ACCESS_TOKEN = open(".mapbox_token").read()
    #print(ACCESS_TOKEN)
    #px.set_mapbox_access_token(ACCESS_TOKEN)


    #map_json = getInititalMap()    


    spark = SparkSession.builder.appName("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    #df = getinitialMapDF(spark,map_json)
    
    #--------------------------------------------------
    #Maps
    indiamapDf = IndiaMapDF(spark)
    ukmapDf = UKMapDF(spark)
    usmapDf = USMapDF(spark)
    russiamapDf = RussiaMapDF(spark)
    allmaps = {}
    allmaps["India"] = indiamapDf
    allmaps["USA"] = usmapDf
    allmaps["UK"] = ukmapDf
    allmaps["Russia"] = russiamapDf
    #------------------------------------------------
    #Total
    indiatotalDf = IndiaTotalDF(spark)
    uktotalDf = UKTotalDF(spark)
    ustotalDf = USTotalDF(spark)
    russiatotalDf = RussiaTotalDF(spark)
    allgraphs = {}
    allgraphs["India"] = indiatotalDf
    allgraphs["USA"] = ustotalDf
    allgraphs["UK"] = uktotalDf
    allgraphs["Russia"] =russiatotalDf
    #----------------------------------------------

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
                            id="cases-chart",
                        
                        ),
                        className="card",
                    ),

                    html.Div(
                        children=dcc.Graph(
                            id="deaths-chart",
                        
                        ),
                        className="card",
                    ),

                    html.Div(
                        children=dcc.Graph(
                            id="recovered-chart",
                        
                        ),
                        className="card",
                    ),

                    dcc.Interval(id = 'update',interval = 2000000,n_intervals = 0),
                ],
                
                className="wrapper",
            ),
        ]
    )
   

    @app.callback([Output("world-live", "figure") , Output("cases-chart", "figure") , Output("deaths-chart", "figure") ,Output("recovered-chart", "figure")],[Input("update", "n_intervals"),Input("country", "value")])
    def update(n_intervals,country):
        print(country)
        curmapdf = allmaps[country]
        curtotaldf = allgraphs[country]
        #curdf.show()
        ndf = curtotaldf.toPandas()
        #x = curtotaldf.select("Date").rdd.flatMap(lambda x: x).collect()
        #y1 = curtotaldf.select("Cases").rdd.flatMap(lambda x: x).collect()
        #y2 = curtotaldf.select("Deaths").rdd.flatMap(lambda x: x).collect()
        #y3 = curtotaldf.select("Recovered").rdd.flatMap(lambda x: x).collect()
        x = ndf['Date']
        y1 = ndf['Cases']
        y2 = ndf['Deaths']
        y3 = ndf['Recovered']
        return updateMap(go,curmapdf,px,ACCESS_TOKEN), updateCases(go,x,y1) , updateDeaths(go,x,y2), updateRecovered(go, x, y3)

    return app.server

    