import asyncio
import logging
import websockets
import itertools

from .kafka_consumers import async_kafka
from .constants import EXCHANGES, FEEDS

# {topic_id: KafkaConsumer}
broadcaster_lock = asyncio.Lock()
topic_broadcasters = {}

# {topic_id: [ClientHandler, ClientHandler ...]}
subscriptions_lock = asyncio.Lock()
client_subscriptions = {}

tasks_lock = asyncio.Lock()
tasks = {}

published_lock = asyncio.Lock()
n_published = 0

connected_lock = asyncio.Lock()
active_subscriptions = 0

async def subscribe(topic_id: str, client):
    global active_subscriptions
    async with broadcaster_lock:
        async with subscriptions_lock:
            if topic_id not in topic_broadcasters.keys():
                await create_topic(topic_id)
                client_subscriptions[topic_id] = [client]
            else:
                if client in client_subscriptions[topic_id]:
                    return
                else: 
                    client_subscriptions[topic_id].append(client)
            async with connected_lock:
                active_subscriptions += 1

async def unsubscribe(topic_id: str, client):
    global active_subscriptions
    async with broadcaster_lock:
        async with subscriptions_lock:
            if client not in client_subscriptions[topic_id]:
                return
            else:
                client_subscriptions[topic_id].remove(client)
            async with connected_lock:
                active_subscriptions -= 1

async def run_topic(topic_id):
    global n_published
    while not topic_broadcasters[topic_id].closed:
        msg = await topic_broadcasters[topic_id].get()
        subs = client_subscriptions[topic_id]
        sockets = map(lambda client: client.get_ws(), subs)
        websockets.broadcast(sockets, msg)
        async with published_lock:
            n_published += 1

def get_backlog():
    return sum([x.size() for x in topic_broadcasters.values()])

async def create_topic(topic_id):
    consumer = await async_kafka.get_consumer(topic_id)
    if not consumer:
        # This will be replaced by a message parser in the future.
        subscriptions_lock.release()
        broadcaster_lock.release()
        return
    topic_broadcasters[topic_id] = consumer

    task = asyncio.create_task(run_topic(topic_id))
    async with tasks_lock:
        tasks[topic_id] = task

async def remove_topic(topic_id):
    """Precondition: Topic is empty"""
    await async_kafka.shutdown_topic(topic_id)

    del client_subscriptions[topic_id]
    del topic_broadcasters[topic_id]
    async with tasks_lock:
        tasks[topic_id].cancel()
    del tasks[topic_id]

special = [
    ("L1", "bybit"),
    ("ohlcv-m1", "bybit"),
    ("indicators", "uniswap")
]

async def prestart():
    for exchange in EXCHANGES:
        for feed in FEEDS:
            topic = exchange + "-" + feed
            skip = False
            for special_feed in special:
                if feed == special_feed[0] and exchange != special_feed[1]:
                    skip = True
            if exchange == "ethereum" and feed != "raw":
                skip = True
            if exchange == "uniswap" and feed not in  ("indicators", "raw"):
                skip = True
            if skip:
                continue 
            await create_topic(topic)
            client_subscriptions[topic] = []

def dump():
    logging.info(f"backlog: {get_backlog()}\tn_published: {n_published}\tactive_subscriptions: {active_subscriptions}")