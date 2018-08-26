#from pyspark import SparkContext, SparkConf
#from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, udf, window

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
    StructField("timestamp", TimestampType()),
    StructField("rawCategory", StringType())])

spark = SparkSession.builder.appName("PythonStreamingRecieverKafka")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0')\
    .config("spark.driver.memory","2G")\
    .getOrCreate()

inData = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("maxOffsetsPerTrigger", 100)\
    .option("subscribe","news").load()
data   = inData.select(from_json(col("value").cast("string"), json_schema).alias("parsed_value"))
data   = data.select("parsed_value.*")

def entity_is_good(entity, lang):
    # check if empty
    if not entity.strip():
        return False

    # check if contains undesired characters
    if not nlps[lang].vocab[entity.replace('.', '').replace('-', '').replace(' ', '').replace('#', '')].is_alpha:
        return False
    
    # check if it's a stopword
    if nlps[lang].vocab[entity].is_stop:
        return False
    
    return True

def extract_entities(body, region):
    if region == 'brazil':
        lang = 'pt'
    elif region == 'united states' or region == 'canada':
        lang = 'en'
    else:
        lang = 'es'
    nlp_model = nlps[lang]
    doc = nlp_model(body.replace('\r', ' ').replace('\n', ' '))
    entities = [ent.text.strip() for ent in doc.ents if entity_is_good(ent.text, lang)]
    return entities
    #return [token.strip() for token in body.replace('\n', ' ').replace('\r', ' ').split() if token.strip()]

extract_entities_udf = udf(extract_entities, ArrayType(StringType()))

data = data.withColumn('entity', explode(extract_entities_udf('body', 'region')))\
    .withWatermark("timestamp", "1 minute")\
    .groupBy(window("timestamp", "2 minutes", "2 minutes"), 'region', 'entity')\
    .count()#\
    #.sort('count', ascending=False)

#data   = data.withColumn('word', explode(split(col('body'), ' ')))\
#    .groupBy('word')\
#    .count()\
#    .sort('count', ascending=False)
#query.printSchema()

#data.writeStream.outputMode("complete").format("console").start().awaitTermination()
data.writeStream.outputMode("append")\
    .format("json")\
    .option("checkpointLocation", "/home/gabriel/dev/bigdata/news-visualization-with-spark/checkpoint") \
    .option("path", "/home/gabriel/dev/bigdata/news-visualization-with-spark/output")\
    .start()\
    .awaitTermination()