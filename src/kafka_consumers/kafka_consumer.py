import sys
import json
import asyncio
import logging
import threading

from datetime import datetime
from configparser import ConfigParser
from confluent_kafka import Consumer, KafkaError, KafkaException 


class AsyncKafkaConsumer():
    def __init__(self, topic):
        self.topic = topic
        start_time = datetime.utcnow().strftime("%Y%m%d-%H%M%S")

        config = ConfigParser()
        config.read("config.ini")
        self.conf = dict(config['CONSUMER'])
        self.conf['client.id'] = topic + '_kafka-python-consumer_' + start_time
        self.conf['group.id'] = topic + '_ws-server-group_' + start_time

        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])
        self.consumer_lock = asyncio.Lock()
        self.queue = asyncio.Queue()
        self.closed = False
        self.quote_no = None
        self.started = False

    def __repr__(self):
        return f"AsyncKafkaConsumer: topic={self.topic}"

    async def get(self):
        return await self.queue.get()
    
    async def run_consumer(self, stop):
        while not stop.is_set() and not self.closed:
            try:
                async with self.consumer_lock:
                    msg = await asyncio.to_thread(self._consume)
                if msg:
                    await self.queue.put(msg)
            except RuntimeError as e:
                logging.error(e)
    
    async def shutdown(self):
        self.closed = True
        async with self.consumer_lock:
            await asyncio.to_thread(self.consumer.shutdown)
    
    def size(self):
        return self.queue.qsize()

    def _consume(self):
        msg = self.consumer.poll(0)
        if msg is None: 
            return
        
        if not self.started:
            self.started = True
            logging.info(f"consumer for topic {self.topic} started consuming")

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                logging.error(f'{self.topic}: %% %s [%d] reached end at offset %d\n' % (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                logging.error(f"{self.topic}: consumer encountered an error")
                raise KafkaException(msg.error())
        else:
            return msg.value()
    
    async def shutdown(self):
        self.consumer.close()

async def main():
    consumer = AsyncKafkaConsumer("kraken-normalised")
    logging.info("Starting consumer...")
    await consumer.run_consumer(threading.Event())

if __name__ == "__main__":
    asyncio.run(main())