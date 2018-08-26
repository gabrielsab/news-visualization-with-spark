#from pyspark import SparkContext, SparkConf
#from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, udf, array

import sys
import os
#import pprint
#import json

import platform
system_name = platform.system()
if system_name == 'Linux':
    os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
else:
    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

from textblob import TextBlob
import spacy

nlps = {}
nlps['en'] = spacy.load('en')
nlps['pt'] = spacy.load('pt')
nlps['es'] = spacy.load('es')

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
data   = data.select("parsed_value.*")

def extract_entities(body, region):
    if region == 'brazil':
        nlp_model = nlps['pt']
    elif region == 'united states' or region == 'canada':
        nlp_model = nlps['en']
    else:
        nlp_model = nlps['es']
    doc = nlp_model(body)
    entities = [ent.text.strip() for ent in doc.ents if (ent.text.strip() and nlp_model.vocab[ent.text.replace('.', '').replace('-', '')].is_alpha and not nlp_model.vocab[ent.text].is_stop)]
    return entities
    #return [token.strip() for token in body.replace('\n', ' ').replace('\r', ' ').split() if token.strip()]

extract_entities_udf = udf(extract_entities, ArrayType(StringType()))

data = data.withColumn('entity', explode(extract_entities_udf('body', 'region')))\
    .groupBy('region', 'entity')\
    .count()\
    .sort('count', ascending=False)

#data   = data.withColumn('word', explode(split(col('body'), ' ')))\
#    .groupBy('word')\
#    .count()\
#    .sort('count', ascending=False)
#query.printSchema()
data.writeStream.outputMode("complete").format("console").start().awaitTermination()