#!/usr/bin/env python
# -*- coding: utf-8 -*-
import paho.mqtt.client as mqtt #import library
import time

MQTT_BROKER = "192.168.56.101"   #specify the broker address, it can be IP of raspberry pi or simply localhost
MQTT_TOPIC = "test_channel" #this is the name of topic
username = "mosquitto"
password = "dietpi"

#global messageReceived
#messageReceived=False

# callback called when client receives a CONNACK response
def on_connect(client, userdata, flags, rc):
    if rc==0:
        client.subscribe(MQTT_TOPIC)
        print("subscribe to {}".format(MQTT_TOPIC))
#    else:
#       syslog.syslog("bad connection {}".format(rc))

# callback called when a PUBLISH message is received
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload.decode("utf-8")))
    
#    global messageReceived
#    messageReceived=True

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
client.username_pw_set(username,password)
client.on_connect = on_connect
client.on_message = on_message

client.connect(MQTT_BROKER)
client.loop_forever()# use this line if you don't want to write any further code. It blocks the code forever to check for data

