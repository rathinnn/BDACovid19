from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, TimestampType, DoubleType
#spark = SparkSession.builder.appName("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "test2") \
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
    .queryName("Trial")\
    .outputMode("Append")\
    .start()

#query.awaitTermination()

#df.select(df.key, get_json_object(df.jstring, '$.f1').alias("c0"), get_json_object(df.jstring, '$.f2').alias("c1") ).collect()
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 trial.py