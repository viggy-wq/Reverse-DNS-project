import sys
import select
import paho.mqtt.client as paho
import time


def on_message(client, userdata, msg):
    print(msg.payload.decode())

client = paho.Client(client_id="test_sub")

if client.connect("localhost", 1883, 60) != 0:
    print("Failed to connect to broker")
    sys.exit(-1)
client.subscribe("JSON")
client.on_message = on_message
client.loop_forever()