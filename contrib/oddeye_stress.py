from datetime import datetime
import pycurl

import time
import threading
import sys
import datetime
import random

insertcount=int(sys.argv[1])
mythreads=int(sys.argv[2])


insertcount=int(sys.argv[1])
mythreads=int(sys.argv[2])

start_time = time.time()
def push_data():
    start_time = time.time()
    start_time = time.time()
    oddeye_uuid = '79f68e1b-ddb3-4065-aec8-bf2eeb9718e8'
    oddeye_server = 'https://barlus.netangels.net/oddeye-barlus/write'
    timestamp = int(datetime.datetime.now().strftime("%s"))
    oddeye_data='{"UUID":"79f68e1b-ddb3-4065-aec8-bf2eeb9718e8","tags":{"timestamp":' + str(timestamp) + \
                ',"host":"tag_hostname","cluster":"vle_vle","type":"tag_type","group":"host_group"},' \
                '"data":{"vle_1":10.0,' \
                '"vle_2":'+str(random.random())+',' \
                '"vle_3":'+str(random.random())+',' \
                '"vle_4":'+str(random.random())+'}}'
    barlus_style = 'UUID=' + oddeye_uuid + '&data='
    send_data = barlus_style + oddeye_data
    c = pycurl.Curl()
    c.setopt(pycurl.URL, oddeye_server)
    c.setopt(pycurl.POST, 1)
    c.setopt(pycurl.POSTFIELDS, send_data)
    c.setopt(pycurl.VERBOSE, 0)
    c.setopt(pycurl.TIMEOUT, 10)
    c.setopt(pycurl.NOSIGNAL, 5)
    c.setopt(pycurl.WRITEFUNCTION, lambda x: None)
    c.setopt(pycurl.USERAGENT, 'PuyPuy v.01')


    for x in range(0, insertcount):
        c.perform()

    print("%s seconds" % (time.time() - start_time))

threads = []
for i in range(mythreads):
    t = threading.Thread(target=push_data)
    threads.append(t)
    t.start()
