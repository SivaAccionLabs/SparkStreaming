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
    conn = happybase.Connection('54.213.152.242')
    if conn:
        print "Connectin Established"

    conn.create_table('siva', {'cf': dict()})
    table = conn.table('siva')
    for line in rdd.collect():
        table.put('id'+str(line.id), {'cf:name': line.name}) 

def second_hieght_sallary(rdd):
    dg = {}
    for line in rdd.collect():
        line = json.loads(line)
        if line['dept_id'] in dg.keys():
            dg[line['dept_id']].append(line['sallary'])
        else:
            dg[line['dept_id']] = []
            dg[line['dept_id']].append(line['sallary'])

    for dept in dg.keys():
        if len(dg[dept]) >=2:
            print str(dept) + " ===>>>" + str(sorted(dg[dept])[1])
        else:
            print str(dept) + " ===>>>" + str(sorted(dg[dept])[0])

if __name__ == '__main__':
    kafka_params = {"metadata.broker.list": "54.218.253.171:9092"}
    sc = SparkContext(appName="sivaApp") 
    ssc = StreamingContext(sc, 5)
    message = KafkaUtils.createDirectStream(ssc, ["maple"], kafka_params)
    actual = message.map(lambda x: x[1])
    actual.foreachRDD(second_hieght_sallary)

    ssc.start()
    ssc.awaitTermination()
