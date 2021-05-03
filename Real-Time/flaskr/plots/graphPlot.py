
def updateCases(go,x,y):
    #df = tdf.sort("Date")
    fig = go.Figure([go.Scatter(
            

            x = x,
            y = y,
            
            
        )])


    fig.update_layout(
        title={
            'text': "Cases",
            'y':0.5,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    return fig
    
def updateDeaths(go, x,y):
    #df = tdf.sort("Date")
    fig = go.Figure([go.Scatter(
            

            x = x,
            y = y,
            
            
        )])


    fig.update_layout(
        title={
            'text': "Deaths",
            'y':0.5,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    return fig

def updateRecovered(go,x,y):
    #df = tdf.sort("Date")
    fig = go.Figure([go.Scatter(
            

            x = x,
            y = y,
            
            
        )])


    fig.update_layout(
        title={
            'text': "Recovered",
            'y':0.5,
            'x':0.5,
            'xanchor': 'center',
            'yanchor': 'top'})
    return fig
    

    
