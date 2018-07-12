#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr 12 12:49:49 2018

@author: ilias
"""
import subprocess
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient

import boto3
from inotify_simple import INotify, flags
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from time import sleep
from datetime import date, datetime
import json
import cv2
from uuid import getnode as get_mac
import time
import psycopg2


import fnmatch, os 	




class shadowCallbackContainer:
    def __init__(self, deviceShadowInstance):
        self.deviceShadowInstance = deviceShadowInstance

    # Custom Shadow callback
    def customShadowCallback_Delta(self, payload, responseStatus, token):
        # payload is a JSON string ready to be parsed using json.loads(...)
        # in both Py2.x and Py3.x
        print("Received a delta message:")
        payloadDict = json.loads(payload)
        deltaMessage = json.dumps(payloadDict["state"])
        print(deltaMessage)
        print("Request to update the reported state...")
        newPayload = '{"state":{"reported":' + deltaMessage + '}}'

        self.deviceShadowInstance.shadowUpdate(newPayload, None, 5)
        print("Sent.")
def customShadowCallback_Update(payload, responseStatus, token):
    # payload is a JSON string ready to be parsed using json.loads(...)
    # in both Py2.x and Py3.x
    if responseStatus == "timeout":
        print("Update request " + token + " time out!")
    if responseStatus == "accepted":
        payloadDict = json.loads(payload)
        print("~~~~~~~~~~~~~~~~~~~~~~~")
        print("Update request with token: " + token + " accepted!")
        print("property: " + str(payloadDict["state"]["desired"]["live"]))
        print("~~~~~~~~~~~~~~~~~~~~~~~\n\n")
    if responseStatus == "rejected":
        print("Update request " + token + " rejected!")

def customShadowCallback_Delete(payload, responseStatus, token):

 # payload is a JSON string ready to be parsed using json.loads(...)
    # in both Py2.x and Py3.x
    if responseStatus == "timeout":
        print("Update request " + token + " time out!")
    if responseStatus == "accepted":
        payloadDict = json.loads(payload)
        print("~~~~~~~~~~~~~~~~~~~~~~~")
        print("Update request with token: " + token + " accepted!")
        print("property: " + str(payloadDict["state"]["desired"]["live"]))
        print("~~~~~~~~~~~~~~~~~~~~~~~\n\n")
    if responseStatus == "rejected":
        print("Update request " + token + " rejected!")

def customShadowCallback_Delete(payload, responseStatus, token):
    if responseStatus == "timeout":
        print("Delete request " + token + " time out!")
    if responseStatus == "accepted":
        print("~~~~~~~~~~~~~~~~~~~~~~~")
        print("Delete request with token: " + token + " accepted!")
        print("~~~~~~~~~~~~~~~~~~~~~~~\n\n")
    if responseStatus == "rejected":
        print("Delete request " + token + " rejected!")

def customCallback(client, userdata, message):

    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")

def get_rootca():

	for file in os.listdir('.certifcations/'):
	    if fnmatch.fnmatch(file, "*.pem"):
	    	print (".certifcations/"+file)
	    	return (".certifcations/"+file)
def get_cert():

	for file in os.listdir('.certifcations/'):
	    if fnmatch.fnmatch(file, '*.pem.crt'):
	    	print (".certifcations/"+file)
	    	return (".certifcations/"+file)
def get_private():

	for file in os.listdir('.certifcations/'):
	    if fnmatch.fnmatch(file, '*.pem.key'):
	    	print (".certifcations/"+file)
	    	return (".certifcations/"+file)






ShadowClient = AWSIoTMQTTShadowClient("ubuntu-desktop")

ShadowClient.configureEndpoint("a1oa9tg9lcso0.iot.eu-west-1.amazonaws.com", 8883)
ShadowClient.configureCredentials(get_rootca(), 
get_private(),get_cert())
ShadowClient.configureAutoReconnectBackoffTime(1, 32, 20)
ShadowClient.configureConnectDisconnectTimeout(10)  # 10 sec
ShadowClient.configureMQTTOperationTimeout(5)  # 5 sec
ShadowClient.connect()
deviceShadowHandler = ShadowClient.createShadowHandlerWithName("Litterbug", True)
shadowCallbackContainer_Bot = shadowCallbackContainer(deviceShadowHandler)
#deviceShadowHandler.shadowRegisterDeltaCallback(shadowCallbackContainer_Bot.customShadowCallback_Delta)


mac = get_mac()
print (mac)

# MQTT Connection establishement
myMQTTClient = AWSIoTMQTTClient("Litterbug_desktop")
myMQTTClient.configureEndpoint("a1oa9tg9lcso0.iot.eu-west-1.amazonaws.com", 8883)
myMQTTClient.configureCredentials(get_rootca(), 
get_private(),get_cert())


myMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec
#connect and publish
myMQTTClient.connect()
s3 = boto3.client('s3', aws_access_key_id= os.environ["ACCESSKEY"],
aws_secret_access_key= os.environ["SECRETKEY"])


con = psycopg2.connect("host='litterbugdb.c1ekrfqx70oj.eu-west-1.rds.amazonaws.com' dbname='littering' user='ilias' password='Algeria7201990'")
cur = con.cursor()
cur.execute("SELECT id FROM devices where mac_addr='%s'",[mac])
row = cur.fetchone()
device_id=row[0]
print (device_id)
s3.download_file('littercam','device-'+str(mac)+'/parameters.txt', '.parameters.txt')

#subprocess.Popen(["./Litter_detect"])
myMQTTClient.publish("topic", "connected", 0)

timestr = time.strftime("%Y-%m-%d-%H:%M:%S")

#timestr = time.strftime("%Y-%m-%d-%H:%M:%S")

#s3.upload_file("/home/pi/detections/litter.jpg", "littercam", "device-"+str(mac)+"/litter"+str(timestr)+".jpg")         

# inotify initialization 

inotify = INotify()
watch_flags = flags.CREATE | flags.MODIFY 
wd = inotify.add_watch('detections', watch_flags)
while 1:
       timestr = time.strftime("%Y-%m-%d-%H:%M:%S")
       JSONPayload = '{"state":{"desired":{"live": \"'+timestr+'\" }}}'
       deviceShadowHandler.shadowUpdate(JSONPayload,customShadowCallback_Update, 5)

       events= inotify.read(3000,200)
       print ("evnets "+ str(events))
       if events: 
           #if litter event send detection image to S3 bucket "littercam"
            image=cv2.imread("detections/litter.bmp");
            cv2.imwrite('detections/litter.jpg',image,[int(cv2.IMWRITE_JPEG_QUALITY), 50])
            timestr = time.strftime("%Y-%m-%d-%H:%M:%S")

            s3.upload_file("detections/litter.jpg", "littercam", "device-"+str(mac)+"/litter"+str(timestr)+".jpg")
            s3.upload_file("detections/litter.jpg", "littercam", "device-"+str(mac)+"/litter.jpg")
           # and send message to the MQTT topic 
            detection= "device-"+str(mac)+"/litter"+str(timestr)+".jpg"
            message = {}
            message['message'] = " A littering event is detected"
            message['imageKey'] = events[-1].name
            message['device_id'] = "ID_1" 
            with open('litterlog.txt', 'a') as the_file:
                    the_file.write("A littering is detected at "+timestr+"\n")         
            s3.upload_file("litterlog.txt", "littercam", "device-"+str(mac)+"/log.txt")
            messageJson = json.dumps(message)
            myMQTTClient.publish("topic", messageJson, 1)
            #cur.execute("UPDATE events SET detection= %s, confidence = %s WHERE device_id = %s",(detection,95,device_id))
            stat=cur.execute("INSERT INTO events ( detection, confidence, device_id) VALUES (%s,%s,%s) ",(detection,95,device_id))
            con.commit()
	
            event=events[-1] 
 
            print(event)
           # for flag in flags.from_mask(event.mask):
            #   print('    ' + str(flag))
            del events
