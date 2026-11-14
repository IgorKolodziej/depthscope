"""Collect raw Kraken WebSocket messages."""

import json
import time

from .feeds import ASSETS

URLS = {"kraken": "wss://ws.kraken.com/v2"}


def subscriptions(venue):
    symbols = [f"{asset}/USD" for asset in ASSETS]
    return [{"method": "subscribe", "params": dict(channel="book", symbol=symbols, depth=1000)},
            {"method": "subscribe", "params": dict(channel="trade", symbol=symbols, snapshot=False)},
            {"method": "subscribe", "params": dict(channel="instrument")}]


async def capture(publish):
    from websockets.asyncio.client import connect

    async with connect(URLS["kraken"], max_size=16 * 1024 * 1024, open_timeout=10) as socket:
        for message in subscriptions("kraken"):
            await socket.send(json.dumps(message))
        async for payload in socket:
            if isinstance(payload, bytes):
                payload = payload.decode()
            receipt = time.time_ns()
            publish(dict(kind="message", receipt_ns=receipt, received_ns=receipt,
                         venue="kraken", payload=payload))
