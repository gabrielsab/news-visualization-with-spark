#from pyspark import SparkContext, SparkConf
#from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, explode, split, udf, window, split, collect_list
# from pyspark import SparkContext, SparkConf

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

nlps 	           = {}
nlps['en']         = spacy.load('en')
nlps['pt']         = spacy.load('pt')
nlps['es'] 		   = spacy.load('es')
checkpointLocation = "/Users/pamelatabak/Documents/PESC_UFRJ/Banco_de_Dados/news-visualization-with-spark/checkpoint"
path 			   = "/Users/pamelatabak/Documents/PESC_UFRJ/Banco_de_Dados/news-visualization-with-spark/output"

json_schema = StructType([
    StructField("body", StringType()),
    StructField("title", StringType()),
    StructField("region", StringType()),
    StructField("summary", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("rawCategory", StringType()),
    StructField("url", StringType())])

spark = SparkSession.builder.appName("PythonStreamingRecieverKafka")\
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0')\
    .config("spark.driver.memory","2G")\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

inData = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("maxOffsetsPerTrigger", 100)\
    .option("subscribe","news").load()
data   = inData.select(from_json(col("value").cast("string"), json_schema).alias("parsed_value"))
data   = data.select("parsed_value.*")#.filter((col("region") != "canada")) #news might be in france, we are not interested 

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

def extract_entities(body, region, url):
    entity_positions  = {}
    sentence_ends 	  = []
    entity_sentiments = {}
    if region == 'brazil':
        lang = 'pt'
    elif region == 'united states':
        lang = 'en'
    else:
        lang = 'es'
    
    blob = TextBlob(body)
    for sentence in blob.sentences:
        sentence_ends.append(sentence.end_index)
    sentence_ends.sort()

    nlp_model = nlps[lang]
    doc 	  = nlp_model(body.replace('\r', ' ').replace('\n', ' '))
    for ent in doc.ents:
        cleaned_ent = ent.text.strip()
        if entity_is_good(cleaned_ent, lang):
            if cleaned_ent not in entity_positions:
                entity_positions[cleaned_ent] = []
            entity_positions[cleaned_ent].append(ent.start_char)
    for entity in entity_positions:
        if entity not in entity_sentiments:
            entity_sentiments[entity] = []
        for position in entity_positions[entity]:
            sentence_idx = 0
            while position > sentence_ends[sentence_idx]:
                sentence_idx += 1
                if sentence_idx > len(sentence_ends) - 1:
                    break
            if sentence_idx <= len(sentence_ends) - 1:
                sentence = blob.sentences[sentence_idx]
                entity_sentiments[entity].append(sentence.sentiment.polarity)
    ret = []
    for ent in entity_sentiments:
        sentiments = entity_sentiments[ent]
        for s in sentiments:
            ret.append((str(ent) + "---" + str(s) + "---" + url))
    return ret

extract_entities_udf = udf(extract_entities, ArrayType(StringType()))

data = data.withColumn('entitysentiment', explode(extract_entities_udf('body', 'region', 'url')))

split_col = split(data['entitysentiment'], '---')
data 	  = data.withColumn('entity', split_col.getItem(0))
data      = data.withColumn('sentiment', split_col.getItem(1))
data      = data.withColumn('url', split_col.getItem(2))
data      = data.select("entity", "sentiment", "url", "region", "timestamp").withWatermark("timestamp", "1 minute")\
    .groupBy(window("timestamp", "2 minutes", "2 minutes"), 'region', 'entity')\
    .agg(collect_list("sentiment"), collect_list("url"))

data.writeStream.outputMode("complete").format("console").start().awaitTermination()
# data.writeStream.outputMode("append")\
#     .format("json")\
#     .option("checkpointLocation", checkpointLocation) \
#     .option("path", path)\
#     .start()\
#     .awaitTermination()