from kafka import KafkaConsumer
from json import loads
import json
import msgpack
import time
import requests
import datetime

# change the argument here
TOPIC = 'Thingsboard'
GROUP = 'python1'
BOOTSTRAP_SERVERS = ['13.251.166.96:9092']
SMS_GATEWAY = 'http://13.251.166.96/api/v1/VUsTDQL36fZTknuxEdDG/telemetry'
CHECK_FREQUENCY = 2
OUTPUT_FREQUENCY = 1

consumer = KafkaConsumer(TOPIC, group_id= GROUP, bootstrap_servers= BOOTSTRAP_SERVERS)

# consume json messages
# KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# consume msgpack
KafkaConsumer(value_deserializer=msgpack.unpackb)

# StopIteration if no message after 1sec
KafkaConsumer(consumer_timeout_ms=1000)

while True:
    try:
        message = consumer.poll(timeout_ms=500,max_records=1)

        if not message:
            time.sleep(CHECK_FREQUENCY)
            continue
        
        for tp, messages in message.items():
            for msg in messages:
                print ("%s:%d:%d: key=%s value=%s" % (tp.topic, tp.partition, msg.offset, msg.key, msg.value.decode('utf-8')))
                payload = json.loads(msg.value.decode('utf-8'))
                i=0
                while i < len(payload['SMS_Recipients']):
                    SMS_body = "https://hgw.hkcsl.com/gateway/gateway.jsp?application=HKT_IOT&mrt="+ payload['SMS_Recipients'][i] +"&msg_utf8="+payload['body']
                    
                    url = SMS_GATEWAY
                    myobj = {'SMS_Request': SMS_body,"request_time":datetime.datetime.now().strftime("%c")}
                    print(json.dumps(myobj))
                    headers = {'content-type': 'application/json'}
                    x = requests.post(url, data = json.dumps(myobj), headers=headers)
                    print(x.text)

                    i += 1
                    time.sleep(OUTPUT_FREQUENCY)
                    print ("sleep")

    except Exception as e:
        print (e)
    except (KeyboardInterrupt, SystemExit):
        raise
    
