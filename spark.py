import sys
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import pprint
import json
from pyspark.sql.functions import col, from_json

import platform
system_name = platform.system()
if system_name == 'Linux':
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
else:
    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-sql-kafka-0-10_2.11-2.1.0.jar spark-streaming-kafka-0-8-assembly_2.11-2.3.1.jar pyspark-shell'

# sc  = SparkContext(appName="PythonStreamingRecieverKafka")
# sc.setLogLevel("WARN")
# ssc = StreamingContext(sc, 2) # 2 second window
json_schema = StructType([
    StructField("body", StringType()),
    StructField("title", StringType()),
    StructField("region", StringType()),
    StructField("summary", StringType()),
    StructField("timestamp", StringType()),
    StructField("rawCategory", StringType())])

spark = SparkSession.builder.appName("PythonStreamingRecieverKafka")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0').getOrCreate()

inData = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","news").load()
data   = inData.select(from_json(col("value").cast("string"), json_schema).alias("parsed_value"))
query  = data.select("parsed_value.*")
query.printSchema()
query.writeStream.outputMode("append").format("console").start().awaitTermination()