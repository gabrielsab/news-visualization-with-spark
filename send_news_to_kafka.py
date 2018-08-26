import pymongo
from bson.objectid import ObjectId
import datetime
import pprint
from kafka_helpers import *
import json

#https://medium.com/m/global-identity?redirectUrl=https%3A%2F%2Ftowardsdatascience.com%2Fgetting-started-with-apache-kafka-in-python-604b3250aa05

#http://api.mongodb.com/python/current/tutorial.html

mongo_username          = ""
mongo_password          = ""
mongo_address           = ""
mongo_database_name     = "FEED"
news_collection_name    = "Feed_work_area_6"
control_collection_name = "Spark_Control"

print("connecting mongo")

client = pymongo.MongoClient('mongodb://' + mongo_username + ":" + mongo_password + "@" + mongo_address)
db     = client[mongo_database_name]

# collection with news
news_collection = db[news_collection_name]

# collection that contains last news enqueued to sqs (so we know where to continue)
control_collection = db[control_collection_name]

print("connecting kafka")
kafka_producer = connect_kafka_producer()

print("searching news to be uploaded")

news = None

document = control_collection.find().sort([("_id", pymongo.DESCENDING)])
if(document.count() == 0):
	#read everything (we need to establish the time period that we are going to use)
	news = news_collection.find({ "TimeStamp" : {"$gte" : datetime.datetime(2018, 8, 8)}})
else:
	# we will enqueue from that _id forward
	news = news_collection.find({ "_id" : {"$gte" : document.next()["_id"]}})

print(news.count())

#now we basically need to iterate through news and enqueue messages
last_id = None

count = 0
for news_object in news:
	count += 1
	if (count % 100 == 0):
		print(count)
	try:
		if (last_id == None or news_object["_id"] > last_id):
			last_id = news_object["_id"]
		# we need to decide which fields to enqueue
		# Links[0].Body, Title.Text, Summary.Text, FeedRegion (country), TimeStamp, RawCategory
		if (("Links" not in news_object or news_object["Links"] == None) or ("Links" in news_object and news_object["Links"] != None and (("Body" not in news_object["Links"][0]) or ("FinalUrl" not in news_object["Links"][0])))):
			continue
		raw_category = ""
		if ("RawCategory" in news_object):
			raw_category = news_object["RawCategory"]
		if (raw_category == None):
			raw_category = ""
		summary = ""
		if ("Summary" in news_object and news_object["Summary"] != None and "Text" in news_object["Summary"]):
			summary = news_object["Summary"]["Text"]
		title = ""
		if ("Title" in news_object and news_object["Title"] != None and "Text" in news_object["Title"]):
			title = news_object["Title"]["Text"]
		news_object_to_enqueue = { 	"body" : news_object["Links"][0]["Body"], "title" : title,
									"summary" : summary, "region" : news_object["FeedRegion"],
									"timestamp" : news_object["TimeStamp"].strftime("%Y-%m-%d %H:%M:%S"), "rawCategory" : raw_category,
									"url" : news_object["Links"][0]["FinalUrl"] }
		news_object_to_enqueue = json.dumps(news_object_to_enqueue)
		publish_message(kafka_producer, 'news', 'raw', news_object_to_enqueue)
	except KeyError:
		print("Key error type while inserting " + str(news_object))

if kafka_producer is not None:
	kafka_producer.close()

#after we are done, we need to insert the last _id on control_collection
control_collection.insert_one({"LastIdEnqueued": last_id, "date": datetime.datetime.utcnow()})