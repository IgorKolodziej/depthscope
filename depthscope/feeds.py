"""Convert archived exchange messages into book, trade and feed events."""

import json
from decimal import Decimal

VENUES = ("kraken",)
ASSETS = ("BTC", "ETH")
NS = 1_000_000_000


def decode(venue, payload):
    message = json.loads(payload, parse_float=Decimal)
    kind = message.get("type")
    if message.get("success") is False:
        raise ValueError(message.get("error", "Kraken subscription error"))
    channel = message.get("channel")
    if channel in ("heartbeat", "status"):
        if channel == "status" and any(row["system"] != "online" for row in message["data"]):
            raise ValueError("Kraken system is not online")
        for asset in ASSETS:
            yield asset, "heartbeat", None
    elif channel == "instrument":
        for pair in message["data"].get("pairs", ()):
            asset = pair["symbol"].removesuffix("/USD")
            if asset in ASSETS:
                yield asset, "instrument", (pair["status"], pair["price_increment"], pair["qty_increment"])
    elif channel in ("book", "trade"):
        for row in message["data"]:
            asset = row["symbol"].removesuffix("/USD")
            if asset not in ASSETS:
                continue
            if channel == "book":
                changes = [(bid, level["price"], level["qty"])
                           for bid, side in ((True, "bids"), (False, "asks"))
                           for level in row.get(side, ())]
                yield asset, "book", (kind == "snapshot", changes, int(row["checksum"]))
            elif kind == "update":
                yield asset, "match", int(row["trade_id"])
