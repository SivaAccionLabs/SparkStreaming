"""

To run
  spark-submit --jars <required jars> job.py

"""
import sys
from operator import add
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import *
import json
from operator import add
import happybase

hbase_table = "maple"
hbase_host = "10.0.0.114"
kafka_broker = "10.0.0.206:9092"
topic = "maple"
appName = "LearningApp"
batch_interval = 5

def write_hbase(dept, second_highest_sallary):
    conn = happybase.Connection(hbase_host)
    if not conn:
        print "Error while Establishing a connection to Hbase"
     
    try:
        conn.create_table(hbase_table, {'cf': dict()})
    except:
        pass
    table = conn.table(hbase_table)
    table.put('dept_'+str(dept), {'cf:second_highest_sallary': str(second_highest_sallary)}) 

def second_hieght_sallary(rdd):
    dg = {}
    for line in rdd.collect():
        line = json.loads(line)
        if line['dept_id'] in dg.keys():
            dg[line['dept_id']].append(line['sallary'])
        else:
            dg[line['dept_id']] = []
            dg[line['dept_id']].append(line['sallary'])

    print "*"*20
    print "\n"
    for dept in dg.keys():
        if len(dg[dept]) >=2:
            print "Dept-" + str(dept) + " ===>>> " + str(sorted(dg[dept])[1])
            write_hbase(dept, sorted(dg[dept])[1])
        else:
            print "Dept-" + str(dept) + " ===>>> " + str(sorted(dg[dept])[0])
            write_hbase(dept, sorted(dg[dept])[0])
    print "\n"
    print "*"*20

if __name__ == '__main__':

    kafka_params = {"metadata.broker.list": kafka_broker}

    # Create a SparkContext
    sc = SparkContext(appName=appName) 

    # Create a StreamingContext
    ssc = StreamingContext(sc, batch_interval)

    # Create a DStream
    message = KafkaUtils.createDirectStream(ssc, [topic], kafka_params)

    # Parse the actual message
    actual = message.map(lambda x: x[1])

    # Call the actual logic function
    actual.foreachRDD(second_hieght_sallary)

    # Start the computation
    ssc.start()
    # Wait for the computation to terminate
    ssc.awaitTermination()
