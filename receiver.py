#spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.1 receiver.py
'''
receive data coming from kafka (producer.py) and insert data into mongodb
'''

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import json
import global_vals
from mongo_utils import mongo_utils

def insert_row(x):
    if x is None or len(x)<1:
        return
    data_list=x.split(',')
    mongo_utils.insert_data({
        'timestamp': data_list[0],
        'uid': data_list[1],
        'heart_rate': data_list[2],
        'steps': data_list[3]
    })

sc=SparkContext(master='local[*]', appName='test')
ssc=StreamingContext(sc, batchDuration=global_vals.data_produce_duration)
brokers='localhost:9092'
topic=global_vals.kafka_topic
kvs=KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list":brokers})
kvs.pprint()
lines=kvs.map(lambda x:'{},{},{},{}'.format(json.loads(x[1])['timestamp'],json.loads(x[1])['uid'],
                                           json.loads(x[1])['heart_rate'],json.loads(x[1])['steps']))
lines.foreachRDD(lambda rdd:rdd.foreach(insert_row))

ssc.start()
ssc.awaitTermination()

