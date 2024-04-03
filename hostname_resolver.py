import sys
import dns.resolver, dns.reversename
import sqlite3
from datetime import datetime, timedelta
import json
import yaml
import select
import time
import asyncio
import logging
from address_subscriber import AddressSubscriber
from hbmqtt.client import MQTTClient
from hbmqtt.mqtt.constants import QOS_1

CREATE_CACHE = '''
            CREATE TABLE IF NOT EXISTS cache (
                ip_address TEXT PRIMARY KEY,
                hostname TEXT,
                timestamp DATETIME
            )
        '''
SELECT_TIMESTAMP = 'SELECT hostname, timestamp FROM cache WHERE ip_address = ?'
INSERT_INTO_CACHE = '''
            INSERT INTO cache (ip_address, hostname, timestamp) 
            VALUES (?, ?, ?)
            ON CONFLICT(ip_address) 
            DO UPDATE SET hostname = ?, timestamp = ?
        '''
CACHE_CLEANUP = 'DELETE FROM cache WHERE timestamp < ?'

class DnsCacheSync:
    def __init__(self, config, q_to_resolver):
        self.db_path = config['database']['path']
        self.dns_server = config['dns']['server']
        self.cache_timeout = config['cache']['timeout']
        self.json_dump_interval = config['jsonDump']['timeout']
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute(CREATE_CACHE)
        self.conn.commit()
        self._q_to_resolver = q_to_resolver
        self.client = MQTTClient(client_id="resolver")
        self.topic = [("JSON", QOS_1)]
        self.connected = False
    
    async def connect(self): # Connect to mqtt to broker for publishing JSON
        maxAttempts = 5
        sleepTimeout = 1
        count = 0

        while True:
            count += 1
            if count > maxAttempts: # Allow for multiple connection attempts before retrying task
                logging.warning(f"Max attempts reached")
                asyncio.ensure_future(self.client.connect())
                return
            try:
                logging.info(f"Attempt #{count} to connect to MQTT broker")
                await self.client.connect("mqtt://localhost")
                self.connected = True
                return
            except asyncio.CancelledError:
                logging.warning(f"Connection attempt cancelled after {count} attempts")
                return
            except Exception as error:
                await asyncio.sleep(sleepTimeout)
                sleepTimeout = min(60, sleepTimeout * 2) # Steadily double sleep timeout


    def getHost(self, ip_address): # Utilize dnspython for resolving un-cached hostnames
        addr = dns.reversename.from_address(ip_address)
        my_resolver = dns.resolver.Resolver()
        my_resolver.nameservers = [self.dns_server]

        try:
            hostname = str(my_resolver.resolve(addr, 'PTR')[0])
        except dns.resolver.NXDOMAIN: # NXDOMAIN exception raised when DNS server fails
            logging.warning("DNS server failed to resolve hostname")
            hostname = "Hostname not found"
        except Exception as e:
            logging.warning(f"Error resolving hostname: {e}")
            hostname = "Hostname not found"
        return hostname

    def checkTimestamp(self, ip_address): # Assert cached result is not older than cache timeout
        cursor = self.conn.cursor()
        cursor.execute(SELECT_TIMESTAMP, (ip_address,))
        res = cursor.fetchone()

        if res:
            hostname, timestamp = res
            timestamp = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            if datetime.now() - timestamp < timedelta(hours=1):
                return hostname
        
        return None

    def updateCache(self, ip_address, hostname):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor = self.conn.cursor()
        cursor.execute(INSERT_INTO_CACHE, (ip_address, hostname, timestamp, hostname, timestamp))
        self.conn.commit()
        return hostname

    async def cacheToJson(self):
        await self.connect()
        logging.info("Starting JSON dump task")
        while True:
            try:
                cursor = self.conn.cursor()
                cursor.execute('SELECT * FROM cache')
                rows = cursor.fetchall()
                cache_list = [{"ip_address": row[0], "hostname": row[1], "timestamp": row[2]} for row in rows]
                jsonDump = json.dumps(cache_list, indent=4)
                await self.client.publish(topic="JSON", message=jsonDump.encode(), qos=QOS_1)
            except Exception as error:
                logging.warning(f"Error: {error}")
                self.conn = sqlite3.connect(self.db_path)
                asyncio.ensure_future(self.cacheToJson())
                return
            await asyncio.sleep(self.json_dump_interval)

    async def cleanupCache(self):
        while True:
            try:
                current_time = datetime.now()
                threshold_time = current_time - timedelta(seconds=self.cache_timeout)

                threshold_time_str = threshold_time.strftime('%Y-%m-%d %H:%M:%S')

                cursor = self.conn.cursor()
                cursor.execute(CACHE_CLEANUP, (threshold_time_str,)) # Cleanup all entries older than cache timeout
                self.conn.commit()

                logging.info("Cache cleanup: Entries older than {} have been removed.".format(timedelta(self.cache_timeout)))
            except Exception as error:
                logging.warning(f"Error: {error}")
                self.conn = sqlite3.connect(self.db_path)
                asyncio.ensure_future(self.cleanupCache())
                return
            await asyncio.sleep(self.cache_timeout)

    def processRequest(self, ip_address):
        hostname = self.checkTimestamp(ip_address)
        if not hostname:
            logging.info("Hostname was not found in cache")
            hostname = self.getHost(ip_address)
            self.updateCache(ip_address, hostname)
        logging.info(f"Processed: {ip_address} -> {hostname}")
    
    async def resolverStart(self):
        while True:
            try:
                ip_address = await self._q_to_resolver.get()
                self.processRequest(ip_address)
            except Exception as error:
                logging.warning(f"Error: {error}") # If exception thrown assume sqlite connection is broken and retry task
                self.conn = sqlite3.connect(self.db_path)
                asyncio.ensure_future(self.resolverStart())
                return

def loadConfig(config_path):
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)    

async def main(config_path):
    config = loadConfig(config_path)
    logging.basicConfig(filename=config['logging']['filename'], format=config['logging']['format'], level=logging.DEBUG)
    logging.info("Log Initialized")

    q_to_resolver = asyncio.Queue(100)
    sub = AddressSubscriber(q_to_resolver)
    db = DnsCacheSync(config, q_to_resolver)
    # Create async tasks to run
    readerTask = asyncio.create_task(sub.reader())
    resolverTask = asyncio.create_task(db.resolverStart())
    cacheDumpTask = asyncio.create_task(db.cacheToJson())
    cleanupTask = asyncio.create_task(db.cleanupCache())
    
    await readerTask
    await resolverTask
    await cacheDumpTask
    await cleanupTask

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Missing config file")
        sys.exit(1)
    config_path = sys.argv[1]
    asyncio.run(main(config_path))

#142.250.189.174 test ip address
#sfo03s24-in-f14.1e100.net. test hostname