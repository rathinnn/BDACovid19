
def updateCases(go,x,y):
    #df = tdf.sort("Date")
    
    fig = go.Figure([go.Scatter(
            

            x = x,
            y = y,
            
            
        )])


    fig.update_layout(
        title={
            'text': "Plot of Total Cases",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis = {
            'title' : 'Date'
        },
        yaxis = {
            'title' : 'Total Cases'
        }    
            
            )
    return fig
    
def updateDeaths(go, x,y):
    #df = tdf.sort("Date")
    fig = go.Figure([go.Scatter(
            

            x = x,
            y = y,
            
            
        )])


    fig.update_layout(
        title={
            'text': "Plot of Total Deaths",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis = {
            'title' : 'Date'
        },
        yaxis = {
            'title' : 'Total Deaths'
        }    
            
            )
    return fig

def updateRecovered(go,x,y):
    #df = tdf.sort("Date")
    fig = go.Figure([go.Scatter(
            

            x = x,
            y = y,
            
            
        )])


    fig.update_layout(
        title={
            'text': "Plot of Total Recovered",
            'y':0.9,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'},
        xaxis = {
            'title' : 'Date'
        },
        yaxis = {
            'title' : 'Total Recovered'
        }    
            
            )
    return fig
    

    
