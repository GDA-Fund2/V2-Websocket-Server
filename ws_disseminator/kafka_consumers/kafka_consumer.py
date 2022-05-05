from confluent_kafka import Consumer, KafkaError, KafkaException 
import sys
import asyncio
import threading
import json
from configparser import ConfigParser

# from ..logger import log

class AsyncKafkaConsumer():
    def __init__(self, topic):
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
        msg = self.consumer.poll(0)
        if msg is None: 
            return

        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                (msg.topic(), msg.partition(), msg.offset()))
            elif msg.error():
                print(self.topic, "consumer encountered an error")
                raise KafkaException(msg.error())
        else:
            return msg.value()
    
    async def shutdown(self):
        self.consumer.close()

current_quote_no = -1 
n_incorrect = 0
total = 0

async def monitor(consumer):
    global current_quote_no, n_incorrect, total
    while True:
        print("Awaiting message...")
        msg = await consumer.get()
        if msg:
            msg_dict = json.loads(message)
            if 'quote_no' in msg_dict.keys():
                if current_quote_no == -1:
                    current_quote_no = msg_dict['quote_no']
                else:
                    if msg_dict['quote_no'] != current_quote_no + 1:
                        # print(f"error rate: {n_incorrect/total * 100}")
                        n_incorrect += 1
                print(current_quote_no)
                current_quote_no = msg_dict['quote_no']
                total += 1

async def main():
    consumer = AsyncKafkaConsumer("kraken-normalised")
    print("Starting consumer...")
    asyncio.create_task(monitor(consumer))
    await consumer.run_consumer(threading.Event())

if __name__ == "__main__":
    asyncio.run(main())
