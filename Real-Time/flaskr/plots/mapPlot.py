def updateMap(go,df,px,n,ACCESS_TOKEN):
    s = df.select("Active").rdd.flatMap(lambda x: x).collect()

    fig = go.Figure(px.scatter_mapbox(
            
            lat= df.select("Lat").rdd.flatMap(lambda x: x).collect(),
            lon= df.select("Lon").rdd.flatMap(lambda x: x).collect(),
            color= s,
            size= s,
            zoom = 4,
            size_max=50,
            
            hover_name= df.select("Province").rdd.flatMap(lambda x: x).collect(),
            hover_data=[s],
            
        ))
    
    fig.update_layout(
    mapbox=dict(
        accesstoken=ACCESS_TOKEN,
        
        zoom=3
        )
    )
    return fig
    

    

  
   