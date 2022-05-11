import json
from .constants import OPS, EXCHANGES, FEEDS

def is_valid(message):
    try:
        message = json.loads(message)
    except ValueError as e:
        print(e)
        return 1

    if not ("op" in message.keys() and
                "exchange" in message.keys() and
                "feed" in message.keys()):
        return 2

    if not message['op'] in OPS:
        return 3
    
    if not message['exchange'] in EXCHANGES:
        return 4
    
    if not message['feed'] in FEEDS:
        return 5
    
    if message['feed'] == "L1" and message['exchange'] != "bybit":
        return 5

    if message['feed'] == "ohlcv-m1" and message['exchange'] != "bybit":
        return 5

    return 0

def is_subscribe(message):
    """Assumes that message is valid"""
    return True if message['op'] == "subscribe" else False

def is_unsubscribe(message):
    """Assumes that message is valid"""
    return True if message['op'] == "unsubscribe" else False