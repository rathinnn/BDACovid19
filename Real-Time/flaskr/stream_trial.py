from pyspark.sql import SparkSession
import json
from pyspark.sql.types import *
from pyspark.sql.functions import *
import plotly
import random
import plotly.graph_objs as go
from dash.dependencies import Output, Input
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

def foreach_batch_function(df, epoch_id):
    global df2
    df2 = df2.union(df)

lines.writeStream.foreachBatch(foreach_batch_function).start()


field = [StructField("value", StringType(), True)]
schema = StructType(field)

from pyspark.sql import SQLContext
df2 = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)

from flask import Flask
app = Flask(__name__)


@app.route('/')
def hello():
    df2.show()
    return "Hello World!"

if __name__ == '__main__':
    app.run()