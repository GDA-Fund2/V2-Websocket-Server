import logging
import asyncio
import threading
from .redis_consumer import AsyncRedisConsumer


# {topic: Event}
events_lock = asyncio.Lock()
events = {}

async def get_consumer(topic):
    consumer = AsyncRedisConsumer(topic)
    event = threading.Event()
    asyncio.create_task(consumer.run_consumer(event))
    events[topic] = {'consumer': consumer, 'event': event}
    logging.info(f"{topic} instantiated")
    return consumer

async def shutdown_topic(topic):
    async with events_lock:
        events[topic]['event'].set()
        await events[topic]['consumer'].shutdown()
        del events[topic]
    logging.info(f"{topic} stopped")