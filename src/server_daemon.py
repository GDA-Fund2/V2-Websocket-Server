import ssl
import signal
import websockets
import asyncio
import functools
import logging
import http

from configparser import ConfigParser
from .client_handler import handle_ws
from . import relay


CONFIG_PATH = "config.ini"

stop = None

async def start_server(arg_port=None):
    global stop
    host, port, cert, key = _read_config()
    if arg_port:
        port = arg_port
    
    stop = asyncio.Future()
    loop = asyncio.get_event_loop()

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(cert, key)
    ssl_context=None

    loop.add_signal_handler(signal.SIGUSR1, lambda: asyncio.create_task(stop_server(10)))
    loop.add_signal_handler(signal.SIGUSR2, lambda: asyncio.create_task(dump(12)))

    await relay.prestart() # Bandage fix - need to find out how make consumers connect to Kafka fast

    server_task = asyncio.create_task(run_server(host, port, ssl_context))
    await stop
    logging.info("shut down")

async def handle_request(path, request_headers):
    if path == "/health":
        return http.HTTPStatus.OK, [], b"OK\n"
    if path == "/ready":
        return http.HTTPStatus.OK, [], b"OK\n"
    

async def run_server(host, port, ssl_context):
    logging.info("listening")
    async with websockets.serve(handle_ws, host, port, ssl=ssl_context, process_request=handle_request):
        await stop

async def stop_server(signum):
    logging.info(f"stop signal (signum {signum}) received.")
    stop.set_result(True)
    logging.info("stopping...")

async def dump(signum):
    logging.info(f"dump signal (signum {signum}) received")
    relay.dump()

def _read_config():
    parser = ConfigParser()
    parser.read(CONFIG_PATH)

    conf = parser['SOCKET']
    host = conf['host']
    port = int(conf['port'])
    cert = conf['cert']
    key = conf['key']
    return host, port, cert, key