from confluent_kafka import Consumer, KafkaError, KafkaException 
import sys
import asyncio
import threading
import json
from configparser import ConfigParser

from ..logger import log

class AsyncKafkaConsumer():
    def __init__(self, topic):
        self.exchange = "-".join(topic.split("-")[:-1])
        self.topic = topic

        config = ConfigParser()
        config.read("config.ini")
        self.conf = dict(config['CONSUMER'])

        self.consumer = Consumer(self.conf)
        self.consumer.subscribe([self.topic])
        self.consumer_lock = asyncio.Lock()
        self.queue = asyncio.Queue()
        self.closed = False
        self.quote_no = None

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
                print(e)
    
    async def shutdown(self):
        self.closed = True
        async with self.consumer_lock:
            await asyncio.to_thread(self.consumer.shutdown)
    
    def size(self):
        return self.queue.qsize()

    def _consume(self):
        msg = self.consumer.poll(timeout=10e-9)
        if msg is None: 
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                log(self.topic, "consumer encountered an error")
                raise KafkaException(msg.error())
        else:
            return msg.value()
    
    async def shutdown(self):
        self.consumer.close()


async def main():
    consumer = AsyncKafkaConsumer("bybit")
    print("Starting consumer...")
    asyncio.create_task(consumer.run_consumer(threading.Event()))
    print("Consumer started...")
    while True:
        print("Awaiting message...")
        msg = await consumer.get()
        if msg:
            print(msg)


if __name__ == "__main__":
    asyncio.run(main())