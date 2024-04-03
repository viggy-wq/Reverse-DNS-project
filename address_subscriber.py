import sys
import select
import asyncio
from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_1
import logging

class AddressSubscriber:
    def __init__(self, q_to_resolver):
        self._client = MQTTClient(client_id="test")
        self._topic = [("ip_address", QOS_1)]
        self._q_to_resolver = q_to_resolver
        self.connected = False

    async def connect(self):
        max_attempts = 5
        sleep_timer = 1
        count = 0

        while True:
            count += 1
            if max_attempts and count > max_attempts:
                raise Exception(f'Max number of tries ({count}) reached') # Allow for max attempts to be reached before exception
            try:
                await self._client.connect("mqtt://localhost")
                logging.info("Connected to MQTT broker")
                await self._client.subscribe(self._topic)
                self.connected = True
                return
 
            except:
                await asyncio.sleep(sleep_timer)
        
    async def reader(self):
        await self.connect()
        logging.info("Starting the MQTT Reader")
        while True:
            try:
                message = await self._client.deliver_message()
                ip_address = message.publish_packet.payload.data.decode() # Subscribe to ip address topic and get address
                try:
                    await self._q_to_resolver.put(ip_address) # Put address in async queue
                    logging.info(f"Recieved: {ip_address}")
                except Exception as error:
                    logging.warning(f"Error: {error}")
            except asyncio.CancelledError:
                logging.warning("MQTT Reader stopping on cancellation request")
                return
            except Exception as error:
                logging.warning(f"Error: {error}")
                asyncio.ensure_future(self.reader())
                return
                
async def main():
    queue = asyncio.Queue(100)
    sub = AddressSubscriber(queue)
    await sub.reader()

if __name__ == "__main__":
    asyncio.run(main())



                

