from pyspark.sql.types import *
from pyspark.sql.functions import *

def getinitialMapDF(spark,map_json):
    json_rdd = spark.sparkContext.parallelize([map_json])
    df = spark.read.json(json_rdd)
    df = df.withColumn("Date",df.Date.cast(TimestampType())).withColumn("Lat",df.Lat.cast(DoubleType())).withColumn("Lon",df.Lon.cast(DoubleType()))  
    df = df.filter(df.Date>"2021-04-18")
    return df

def getMapStreamingDF(spark)