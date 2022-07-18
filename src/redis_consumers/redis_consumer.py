import sys
import json
import asyncio
import logging
import threading
import aioredis

from datetime import datetime
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException 
from dotenv import dotenv_values

def get_config():
    conf_path = "config.ini"
    secret_path = "keys/.env"
    config = ConfigParser()
    config.read(conf_path)
    return {
        **config['REDIS'],
        **dotenv_values(secret_path)
    }

class AsyncRedisConsumer():
    def __init__(self, topic):
        self.topic = topic
        start_time = datetime.utcnow().strftime("%Y%m%d-%H%M%S")
        self.id = f"{topic}_redis-python-consumer_{start_time}"
        # config = ConfigParser()
        # config.read("config.ini")
        # self.conf = dict(config['CONSUMER'])
        # self.conf['client.id'] = topic + '_kafka-python-consumer_' + start_time
        # self.conf['group.id'] = topic + '_ws-server-group_' + start_time
        self.conf = get_config()

        # self.consumer = Consumer(self.conf)
        # self.consumer.subscribe([self.topic])
        self.consumer = self.get_redis_pool()
        self.consumer_lock = asyncio.Lock()
        self.queue = asyncio.Queue()
        self.closed = False
        self.quote_no = None
        self.started = False

    def __repr__(self):
        return f"AsyncKafkaConsumer: topic={self.topic}"

    def get_redis_pool(self):
        try:
            pool = aioredis.from_url(
                (f"redis://{self.conf['REDIS_HOST']}"), encoding='utf-8', decode_responses=True)
            return pool
        except ConnectionRefusedError as e:
            print('cannot connect to redis on:', self.redis_host, self.redis_port)
            return None

    async def get(self):
        return await self.queue.get()
    
    async def _push_messages_to_queue(self, messages):
        values = messages[0][1]
        for value in values:
            await self.queue.put(list(value[1].values())[0])

    async def run_consumer(self, stop):
        while not stop.is_set() and not self.closed:
            try:
                async with self.consumer_lock:
                    messages = await self._consume()
                if messages:
                    await self._push_messages_to_queue(messages)
            except RuntimeError as e:
                logging.error(e)
    
    async def shutdown(self):
        self.closed = True
        async with self.consumer_lock:
            await self.consumer.shutdown()
    
    def size(self):
        return self.queue.qsize()

    async def _consume(self):
        try:
            msg = await self.consumer.xreadgroup('ws-server-group', self.id, streams={self.topic: ">"}, count=int(self.conf['stream_max_len']), block=0)
            if msg is None: 
                return
        except Exception as e:
            logging.warning(e)
            await self.consumer.xgroup_create(self.topic, 'ws-server-group', 0, mkstream=True)
            msg = await self.consumer.xreadgroup('ws-server-group', self.id, streams={self.topic: ">"}, count=int(self.conf['stream_max_len']), block=0)
            if msg is None: 
                return
        
        if not self.started:
            self.started = True
            logging.info(f"{self.topic} consuming")

        #print(json.dumps(msg, indent=4))

        return msg
    
    async def shutdown(self):
        await self.consumer.disconnect()

async def main():
    consumer = AsyncRedisConsumer("binance-normalised")
    logging.info("Starting consumer...")
    await consumer.run_consumer(threading.Event())

if __name__ == "__main__":
    asyncio.run(main())