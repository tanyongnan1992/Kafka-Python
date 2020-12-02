from kafka import KafkaConsumer
from json import loads
import json
import msgpack
import time
import requests
import datetime

consumer = KafkaConsumer('Thingsboard', group_id= 'python', bootstrap_servers= ['13.251.166.96:9092'])

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
            time.sleep(2)
            continue
        
        for tp, messages in message.items():
            for msg in messages:
                print ("%s:%d:%d: key=%s value=%s" % (tp.topic, tp.partition, msg.offset, msg.key, msg.value.decode('utf-8')))
                payload = json.loads(msg.value.decode('utf-8'))
                i=0
                while i < len(payload['SMS_Recipients']):
                    SMS_body = "https://hgw.hkcsl.com/gateway/gateway.jsp?application=HKT_IOT&mrt="+ payload['SMS_Recipients'][i] +"&msg_utf8="+payload['body']
                    
                    url = 'http://13.251.166.96/api/v1/VUsTDQL36fZTknuxEdDG/telemetry'
                    myobj = {'SMS_Request': SMS_body,"request_time":datetime.datetime.now().strftime("%c")}
                    print(json.dumps(myobj))
                    headers = {'content-type': 'application/json'}
                    x = requests.post(url, data = json.dumps(myobj), headers=headers)
                    print(x.text)

                    i += 1
                    time.sleep(1)
                    print ("sleep 1s")

    except Exception as e:
        print (e)
    except (KeyboardInterrupt, SystemExit):
        raise
    
