import sys
from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import *
import json
from operator import add
import happybase

def write_hbase(rdd):
    conn = happybase.Connection('10.0.0.96')
    if conn:
        print "Connectin Established"

    #conn.create_table('siva2', {'cf': dict()})
    table = conn.table('siva2')
    print "###########"
    print rdd.collect()
    for line in rdd.collect():
        print line.id
        print line.name
        table.put('id'+str(line.id), {'cf:name': line.name}) 

if __name__ == '__main__':
    print "Creating spark context"
    kafka_params = {"metadata.broker.list": "10.0.0.70:9092"}
    sc = SparkContext(appName="siva") 
    ssc = StreamingContext(sc, 10)
    message = KafkaUtils.createDirectStream(ssc, ["hbase"], kafka_params)
    actual = message.map(lambda x: x[1])
    values = actual.map(lambda x: (json.loads(x)['id'], json.loads(x)['name']))
    values = values.map(lambda data: Row(id=data[0], name=data[1]))
    #values.pprint()
    values.foreachRDD(write_hbase)  

    #actual.pprint()
    #counts = actual.map(lambda x: (json.loads(x)['id'], json.loads(x)['name']))
    #ids = counts.map(lambda x: x[0])
    #names = counts.map(lambda x: x[1])
    #sum = ids.reduce(lambda x,y: x+y)
    #sum.pprint()
    #lines = message.map(lambda line: line[1])
    #words = lines.flatMap(lambda line: line.split(" "))
    #count = words.map(lambda word: (word, 1))
    #count.pprint()
    #word_count = count.reduceByKey(lambda x,y: x+y)
    #word_count.pprint()

    ssc.start()
    ssc.awaitTermination()
