import dash
import dash_core_components as dcc
import dash_bootstrap_components as dbc
import dash_html_components as html
import pandas as pd
import plotly.express as px
#import findspark
#findspark.init()
from pyspark.sql import SparkSession

import plotly
import random
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State

from plots.mapPlot import updateMap
from plots.graphPlot import updateCases, updateDeaths, updateRecovered
from getData import getInititalMap
from SparkJobs import getinitialMapDF, startMapStreamingDF, IndiaMapDF, USMapDF, UKMapDF, RussiaMapDF
from SparkJobs import IndiaTotalDF, UKTotalDF, USTotalDF, RussiaTotalDF, getwordsdf
from KafkaJobs import getConsumer,getDict, getGenerator

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
    #Latest
    allconsumers = {}
    allconsumers["India"] = getConsumer("India")
    allconsumers["USA"] = getConsumer("USA")
    allconsumers["Russia"] = getConsumer("Russia")
    allconsumers["UK"] = getConsumer("UK")

    allgenerators = {}
    allgenerators["India"] = getGenerator(allconsumers["India"])
    allgenerators["USA"] = getGenerator(allconsumers["USA"])
    allgenerators["Russia"] = getGenerator(allconsumers["Russia"])
    allgenerators["UK"] = getGenerator(allconsumers["UK"])

    allprevdicts = {}
    allprevdicts["India"] = {}
    allprevdicts["USA"] = {}
    allprevdicts["Russia"] = {}
    allprevdicts["UK"] = {}

    wordsdf = getwordsdf(spark)
    
    print("Start Kafka Now")
    countries = ["India","USA","UK","Russia"]

    SIDEBAR_STYLE = {
        'position': 'fixed',
        'top': 0,
        'left': 0,
        'bottom': 0,
        'width': '20%',
        'padding': '20px 10px',
        'background-color': '#f8f9fa'
    }

    CARD_STYLE = {
        
    }

    # the style arguments for the main content page.
    CONTENT_STYLE = {
        'margin-left': '25%',
        'margin-right': '5%',
        'padding': '20px 10p'
    }

    TEXT_STYLE = {
        'textAlign': 'center',
        'color': '#191970'
    }

    CARD_TEXT_STYLE = {
        'textAlign': 'center',
        'color': '#0074D9'
    }

    controls = dbc.FormGroup(
        [
            html.P('Country', style={
                'textAlign': 'center',
                
            }),
            dcc.Dropdown(
                id="country",
                options=[
                    {"label": country, "value": country}
                    for country in countries
                ],
                value="India",
                
                
            ),
            html.Div([dcc.Interval(id = 'update',interval = 2000000,n_intervals = 0)]),
            html.Div([dcc.Interval(id = 'update2',interval = 600000,n_intervals = 0)]),
            html.Div([dcc.Interval(id = 'update3',interval = 100,n_intervals = 0)])
        ]
    )

    sidebar = html.Div(
        [
            html.Hr(),
            controls
        ],
        style=SIDEBAR_STYLE,
    )

    content_first_row = dbc.Row([
        dbc.Col(
            dbc.Card(
                [

                    dbc.CardBody(
                        [
                            html.H4(id='Total_Cases', children=['Total Cases'], className='card-title',
                                    style=CARD_TEXT_STYLE),
                            html.P(id='card_cases', children=['number'], style=CARD_TEXT_STYLE),
                        ]
                    )
                ],
                style = CARD_STYLE
            ),
            md=4
        ),
        dbc.Col(
            dbc.Card(
                [

                    dbc.CardBody(
                        [
                            html.H4(id='Total_Deaths', children=['Total Deaths'], className='card-title',
                                    style=CARD_TEXT_STYLE),
                            html.P(id='card_deaths', children=['number'], style=CARD_TEXT_STYLE),
                        ]
                    ),
                ],
                style = CARD_STYLE

            ),
            md=4
        ),
        dbc.Col(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H4(id='Total_Recovered', children=['Total Recovered'], className='card-title',
                                    style=CARD_TEXT_STYLE),
                            html.P(id='card_recovered', children=['number'], style=CARD_TEXT_STYLE),
                        ]
                    ),
                ],
                style = CARD_STYLE

            ),
            md=4
        )
    ])


    content_first_row2 = dbc.Row([

        dbc.Col(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H4(id='Fatality_rate', children=['Fatality Rate'], className='card-title',
                                    style=CARD_TEXT_STYLE),
                            html.P(id='fatality_rate', children=['number'], style=CARD_TEXT_STYLE),
                        ]
                    ),
                ],
                style = CARD_STYLE

            ),
            md=4
        ),
        dbc.Col(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H4(id='Active_cases', children=['Active cases'], className='card-title',
                                    style=CARD_TEXT_STYLE),
                            html.P(id='active_cases', children=['number'], style=CARD_TEXT_STYLE),
                        ]
                    ),
                ],
                style = CARD_STYLE
            ),
            md=4
        ),
        dbc.Col(
            dbc.Card(
                [
                    dbc.CardBody(
                        [
                            html.H4(id='Total_words', children=['Total Words (Testing)'], className='card-title',
                                    style=CARD_TEXT_STYLE),
                            html.P(id='card_words', children=['number'], style=CARD_TEXT_STYLE),
                        ]
                    ),
                ],
                style = CARD_STYLE
            ),
            md=4
        )
    ])

    content_second_row = dbc.Row(
        [
            dbc.Col(
                dcc.Graph(id="world-live"), md=12
            )

        ]
    )

    content_third_row = dbc.Row(
        [
            dbc.Col(
                dcc.Graph(id='cases-chart'), md=12
            )
        ]
    )

    content_fourth_row = dbc.Row(
        [
            dbc.Col(
                dcc.Graph(id='deaths-chart'), md=12
            )
        ]
    )

    content_fifth_row = dbc.Row(
        [
            dbc.Col(
                dcc.Graph(id='recovered-chart'), md=12
            )
        ]
    )

    content = html.Div(
        [
            html.H2('Covid Dashboard', style=TEXT_STYLE),
            html.Hr(),
            content_first_row,
            html.Hr(),
            content_first_row2,
            html.Hr(),
            content_second_row,
            html.Hr(),
            content_third_row,
            html.Hr(),
            content_fourth_row,
            html.Hr(),
            content_fifth_row
        ],
        style=CONTENT_STYLE
    )

    app = dash.Dash(server = server,routes_pathname_prefix="/dashboard/",external_stylesheets=[dbc.themes.BOOTSTRAP])
    
    app.layout = html.Div([sidebar, content])

    @app.callback([Output("world-live", "figure") , Output("cases-chart", "figure") , Output("deaths-chart", "figure") ,Output("recovered-chart", "figure")],[Input("update", "n_intervals"),Input("country", "value")])
    def update(n_intervals,country):
        print(country)
        curmapdf = allmaps[country]
        curtotaldf = allgraphs[country]
        #print(curtotaldf.count())
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

    @app.callback([Output("card_cases", "children") , Output("card_deaths", "children") , Output("card_recovered", "children") , Output("fatality_rate", "children") , Output("active_cases", "children")],[Input("update2", "n_intervals"),Input("country", "value")])
    def update2(n_intervals,country):
        curgenerator = allgenerators[country]
        curprevdict = allprevdicts[country]
        curdict , success = getDict(curgenerator,curprevdict)

        if(success):
            allprevdicts[country] = curdict
        else:
            allgenerators[country] = getGenerator(allconsumers[country])
        
        #print(curdict)
        return (curdict.value['TotalCases'],curdict.value['TotalDeaths'],curdict.value['TotalRecovered'],curdict.value['Case_Fatality_Rate'],curdict.value['ActiveCases'])

    @app.callback([Output("card_words", "children")],[Input("update3", "n_intervals")])
    def update3(n_intervals):
        #wordsdf.show()
        return [wordsdf.count()]

    return app.server







