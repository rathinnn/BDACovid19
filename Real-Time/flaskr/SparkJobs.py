from pyspark.sql.types import *
from pyspark.sql.functions import *


def getinitialMapDF(spark,map_json):
    json_rdd = spark.sparkContext.parallelize([map_json])
    df = spark.read.json(json_rdd)
    df = df.withColumn("Date",df.Date.cast(TimestampType())).withColumn("Lat",df.Lat.cast(DoubleType())).withColumn("Lon",df.Lon.cast(DoubleType()))  
    df = df.filter(df.Date>"2021-04-18")
    return df

def startMapStreamingDF(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest") \
        .option("subscribe", "test2") \
        .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Lat",DoubleType(),True), \
        StructField("Lon",DoubleType(),True), \
        StructField("Province",StringType(),True), \
        StructField("Active",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Lat", "value.Lon","value.Active","value.Province","value.Date")
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("Trial")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("Trial")
    #query.awaitTermination()
    return df

def IndiaMapDF(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest") \
        .option("subscribe", "indiaMaptopic") \
        .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Lat",DoubleType(),True), \
        StructField("Lon",DoubleType(),True), \
        StructField("Province",StringType(),True), \
        StructField("Active",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Lat", "value.Lon","value.Active","value.Province","value.Date")
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("indiaMaptable")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("indiaMapTable")
    #query.awaitTermination()
    return df

def USMapDF(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest") \
        .option("subscribe", "usMaptopic") \
        .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Lat",DoubleType(),True), \
        StructField("Lon",DoubleType(),True), \
        StructField("Province",StringType(),True), \
        StructField("Active",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Lat", "value.Lon","value.Active","value.Province","value.Date")
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("usMapTable")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("usMapTable")
    #query.awaitTermination()
    return df

def UKMapDF(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest") \
        .option("subscribe", "ukMaptopic") \
        .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Lat",DoubleType(),True), \
        StructField("Lon",DoubleType(),True), \
        StructField("Province",StringType(),True), \
        StructField("Active",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Lat", "value.Lon","value.Active","value.Province","value.Date")
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("ukMapTable")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("ukMapTable")
    #query.awaitTermination()
    return df

def RussiaMapDF(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("startingOffsets", "latest") \
        .option("subscribe", "russiaMaptopic") \
        .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Lat",DoubleType(),True), \
        StructField("Lon",DoubleType(),True), \
        StructField("Province",StringType(),True), \
        StructField("Active",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Lat", "value.Lon","value.Active","value.Province","value.Date")
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("russiaMapTable")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("russiaMapTable")
    #query.awaitTermination()
    return df

def IndiaTotalDF(spark):
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("startingOffsets", "latest") \
    .option("subscribe", "indiaTotalTopic") \
    .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Cases",IntegerType(),True), \
        StructField("Deaths",IntegerType(),True), \
        StructField("Recovered",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Date", "value.Cases","value.Deaths","value.Recovered").dropDuplicates(["Date"])
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("indiaTotalTable")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("indiaTotalTable").sort("Date")
    return df

def USTotalDF(spark):
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("startingOffsets", "latest") \
    .option("subscribe", "usTotalTopic") \
    .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Cases",IntegerType(),True), \
        StructField("Deaths",IntegerType(),True), \
        StructField("Recovered",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Date", "value.Cases","value.Deaths","value.Recovered").dropDuplicates(["Date"])
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("usTotalTable")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("usTotalTable").sort("Date")
    return df

def UKTotalDF(spark):
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("startingOffsets", "latest") \
    .option("subscribe", "ukTotalTopic") \
    .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Cases",IntegerType(),True), \
        StructField("Deaths",IntegerType(),True), \
        StructField("Recovered",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Date", "value.Cases","value.Deaths","value.Recovered").dropDuplicates(["Date"])
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("ukTotalTable")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("ukTotalTable").sort("Date")
    return df

def RussiaTotalDF(spark):
    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("startingOffsets", "latest") \
    .option("subscribe", "russiaTotalTopic") \
    .load()
    schema = StructType([ \
        StructField("Date",TimestampType(),True),\
        StructField("Cases",IntegerType(),True), \
        StructField("Deaths",IntegerType(),True), \
        StructField("Recovered",IntegerType(),True)
        ])
        #StructField("index",IntegerType(),True), \

    df2 = df.selectExpr("CAST(value AS STRING)")
    df2.printSchema()
    schemad = df2.select( from_json(df2.value,schema).alias('value') )
    schemad.printSchema()
    schemad2 = schemad.selectExpr("value.Date", "value.Cases","value.Deaths","value.Recovered").dropDuplicates(["Date"])
    #schmead2 = schemad2.withColumn("Lat",schemad2.Lat.cast(DoubleType())).withColumn("Lon",schemad2.Lon.cast(DoubleType()))  
    schemad2.printSchema()
    query = schemad2 \
        .writeStream \
        .format("memory") \
        .queryName("russiaTotalTable")\
        .outputMode("Append")\
        .start()
    df = spark.read.table("russiaTotalTable").sort("Date")
    return df