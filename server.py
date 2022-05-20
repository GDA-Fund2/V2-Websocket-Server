import sys
import asyncio
import logging

from src.server_daemon import start_server

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s",
)

if __name__ == "__main__":
    if len(sys.argv) == 2:
        port = int(sys.argv[1])
    else:
        port = None
    asyncio.run(start_server(port), debug=False)