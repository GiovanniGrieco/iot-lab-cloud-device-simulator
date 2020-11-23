#!/bin/env python3
import json
import random
import ssl
import time

import paho.mqtt.client as mqtt

from opendata import retriever

CLIENT_ID = 'MQTTPublisherTest'
IOT_HUB_HOSTNAME = 'iot-hub-mds'
SAS_TOKEN = 'SharedAccessSignature sr=iot-hub-mds.azure-devices.net%2Fdevices%2FMQTTPublisher&sig=67tgSPxaZjxeSaz9XBwwgutjMNoXCL8loEACKNPYRsA%3D&se=1606136205'

# Callback shen the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print('Connected with result code' + str(rc))

    client.publish(f'devices/{CLIENT_ID}/messages/events/', 'Hello world!')

client = mqtt.Client(CLIENT_ID)
client.on_connect = on_connect

client.username_pw_set(username=f'{IOT_HUB_HOSTNAME}.azure-devices.net/{CLIENT_ID}/?api-version=2018-06-30',
                       password=SAS_TOKEN)
client.tls_set(ca_certs='./azure.pem',
               certfile=None,
               keyfile=None,
               cert_reqs=ssl.CERT_REQUIRED,
               tls_version=ssl.PROTOCOL_TLSv1_2,
               ciphers=None)
client.tls_insecure_set(False)
client.connect(f'{IOT_HUB_HOSTNAME}.azure-devices.net', 8883, 60)
client.loop_start()

while not client.is_connected():
    time.sleep(1)

csv_file_urls = retriever.retrieve_csv_files()
samples = retriever.retrieve_csv_data(csv_file_urls[0])

for sample in samples:
    mqtt_payload = json.dumps(sample)

    print(f'Publishing message to MQTT: {mqtt_payload}')
    client.publish(f'devices/{CLIENT_ID}/messages/events/', mqtt_payload)

    time.sleep(random.randint(5, 30))
