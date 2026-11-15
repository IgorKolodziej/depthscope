"""Collect Kraken WebSocket events in receipt order."""

import asyncio
import json
import logging
import time

from .feeds import ASSETS, NS
from .loadguard import Overload

URLS = {"kraken": "wss://ws.kraken.com/v2"}


def subscriptions(venue):
    symbols = [f"{asset}/USD" for asset in ASSETS]
    return [{"method": "subscribe", "params": dict(channel="book", symbol=symbols, depth=1000)},
            {"method": "subscribe", "params": dict(channel="trade", symbol=symbols, snapshot=False)},
            {"method": "subscribe", "params": dict(channel="instrument")}]


async def feed(venue, events, reconnect):
    from websockets.asyncio.client import connect

    delay = 1
    while True:
        try:
            reconnect.clear()
            async with connect(URLS[venue], max_size=16 * 1024 * 1024, open_timeout=10) as socket:
                await events.put((time.time_ns(), "connected", venue, ""))
                for message in subscriptions(venue):
                    await socket.send(json.dumps(message))
                while True:
                    if reconnect.is_set():
                        break
                    try:
                        payload = await asyncio.wait_for(socket.recv(), timeout=1)
                    except TimeoutError:
                        # The processor handles application-level staleness and requests a resync.
                        continue
                    if isinstance(payload, bytes):
                        payload = payload.decode()
                    await events.put((time.time_ns(), "message", venue, payload))
                    delay = 1
        except (OSError, TimeoutError, UnicodeError) as exc:
            logging.warning("%s feed: %s", venue, exc)
        except Exception:
            logging.exception("%s feed disconnected", venue)
        await events.put((time.time_ns(), "disconnected", venue, ""))
        await asyncio.sleep(delay)
        delay = min(delay * 2, 30)


async def capture(publish, resync_requests):
    events = asyncio.Queue(maxsize=4096)
    reconnect = {venue: asyncio.Event() for venue in URLS}
    tasks = [asyncio.create_task(feed(venue, events, reconnect[venue])) for venue in URLS]
    next_tick = (time.time_ns() // NS + 1) * NS
    last = next_tick - NS
    try:
        while True:
            for venue in resync_requests():
                if venue in reconnect:
                    reconnect[venue].set()
            try:
                event = events.get_nowait()
            except asyncio.QueueEmpty:
                wait = max(0, (next_tick - time.time_ns()) / NS)
                try:
                    event = await asyncio.wait_for(events.get(), timeout=min(wait, 0.25))
                except TimeoutError:
                    event = None
            wall = time.time_ns()
            now = max(last, event[0] if event else wall)
            if event and wall - event[0] > 10 * NS:
                raise Overload(f"capture queue delayed {(wall - event[0]) / NS:.1f}s, depth={events.qsize()}")
            while next_tick <= now:
                publish(dict(kind="tick", receipt_ns=next_tick, venue="", payload=""))
                next_tick += NS
            if event:
                received, kind, venue, payload = event
                publish(dict(kind=kind, receipt_ns=now, received_ns=received, venue=venue, payload=payload))
            last = now
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
