## Kafka-Python-Thingsboard

# *kafka_python-master*
The kafka python module lib
Import to your python program

# *SMS Controller*
Thingsboard push the message included alarm and SMS recipients to a kafka topic  
This SMS controller poll the message in a certain interval, and reassamble the SMS request and send to SMS gateway  
*pm2 start SMSController.py --interpreter python3*  
Check the document inside for more details  
**Current Stage**
Simulate the sensor reading in Node-Red and send to UAT Thingsboard  
UAT Thingsboard generate the alarm and SMS recipients, then push the message to local kafka  
SMS controller reassamble the SMS request and send back to Thingsboard for logging  

# *KafkaPythonMonitor*
**Current Stage**
Check current lag of a topic every 60s and post the lag to Thingsboard for logging  
*pm2 start KafkaPythonMonitor.py --interpreter python3*  