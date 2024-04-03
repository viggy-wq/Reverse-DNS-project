import sys
import select
import paho.mqtt.client as paho
import time

ip_addresses = [
    "8.8.8.8", "8.8.4.4",  # Google Public DNS
    "1.1.1.1", "1.0.0.1",  # Cloudflare DNS
    "208.67.222.222", "208.67.220.220",  # OpenDNS
    "64.6.64.6", "64.6.65.6",  # Verisign DNS
    "9.9.9.9", "149.112.112.112",  # Quad9
    "77.88.8.8", "77.88.8.1",  # Yandex.DNS
    "84.200.69.80", "84.200.70.40",  # DNS.WATCH
    "8.26.56.26", "8.20.247.20",  # Comodo Secure DNS
    "195.46.39.39", "195.46.39.40",  # SafeDNS
    "69.195.152.204", "23.253.163.53"  # Alternate DNS
]

def on_message(client, userdata, msg):
    print(msg.payload.decode())

client = paho.Client(client_id="test_publish")

if client.connect("localhost", 1883, 60) != 0:
    print("Failed to connect to broker")
    sys.exit(-1)

for ip_address in ip_addresses:
    client.publish("ip_address", ip_address)
    time.sleep(.1)


