import json
import logging

from .constants import OPS, EXCHANGES, FEEDS

INVALID_MESSAGE = 1
NOT_ALL_FIELDS_PRESENT = 2
INVALID_OP = 3
INVALID_EXCHANGE = 4
INVALID_FEED = 5
SUCCESS = 0

def is_valid(message):
    try:
        message = json.loads(message)
    except ValueError as e:
        return INVALID_MESSAGE

    if not ("op" in message.keys() and
                "exchange" in message.keys() and
                "feed" in message.keys()):
        return NOT_ALL_FIELDS_PRESENT

    if not message['op'] in OPS:
        return INVALID_OP
    
    if not message['exchange'] in EXCHANGES:
        return INVALID_EXCHANGE
    
    if not message['feed'] in FEEDS:
        return INVALID_FEED
    
    if message['feed'] == "L1" and message['exchange'] != "bybit":
        return INVALID_FEED

    if message['feed'] == "ohlcv-m1" and message['exchange'] != "bybit":
        return INVALID_FEED

    if message['exchange'] == "uniswap" and message['feed'] not in ("indicators", "raw"):
        return INVALID_FEED
    
    if message['exchange'] == "ethereum" and message['feed'] != "raw":
        return INVALID_FEED

    return SUCCESS

def is_subscribe(message):
    """Assumes that message is valid"""
    return True if message['op'] == "subscribe" else False

def is_unsubscribe(message):
    """Assumes that message is valid"""
    return True if message['op'] == "unsubscribe" else False