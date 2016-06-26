#!/usr/bin/python

from datetime import datetime
import time
import threading
import sys
import datetime
import random
from kafka import KafkaProducer

insertcount=int(sys.argv[1])
mythreads=int(sys.argv[2])

start_time = time.time()
oddeye_uuid = '79f68e1b-ddb3-4065-aec8-bf2eeb9718e8'
topic='oddeye'

def push_data():
    timestamp = int(datetime.datetime.now().strftime("%s"))
    producer = KafkaProducer(bootstrap_servers='node0:9092')
    oddeye_data='{"UUID":"79f68e1b-ddb3-4065-aec8-bf2eeb9718e8","tags":{"timestamp":' + str(timestamp) + \
                ',"host":"tag_hostname","cluster":"vle_vle","type":"tag_type","group":"host_group"},' \
                '"data":{"vle_1":10.0,' \
                '"vle_2":'+str(random.random())+',' \
                '"vle_3":'+str(random.random())+',' \
                '"vle_4":'+str(random.random())+'}}'

    for x in range(0, insertcount):
        producer.send(topic, oddeye_data)
        producer.flush()

    print("%s seconds" % (time.time() - start_time))

threads = []
for i in range(mythreads):
    t = threading.Thread(target=push_data)
    threads.append(t)
    t.start()


