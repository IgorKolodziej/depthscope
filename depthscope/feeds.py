"""Convert archived exchange messages into book, trade and feed events."""

import json
from decimal import Decimal

VENUES = ("coinbase", "kraken")
ASSETS = ("BTC", "ETH")
NS = 1_000_000_000


def decode(venue, payload):
    message = json.loads(payload, parse_float=Decimal)
    kind = message.get("type")
    if venue == "coinbase":
        asset = message.get("product_id", "").removesuffix("-USD")
        if kind == "error":
            raise ValueError(message.get("message", "Coinbase subscription error"))
        if kind == "status":
            for product in message["products"]:
                symbol = product["id"].removesuffix("-USD")
                if symbol in ASSETS:
                    status = product["status"]
                    for flag in ("cancel_only", "post_only", "limit_only", "trading_disabled"):
                        if product.get(flag):
                            status = flag
                            break
                    yield symbol, "instrument", (status, product["quote_increment"], product["base_increment"])
            return
        if asset not in ASSETS:
            return
        if kind == "snapshot":
            changes = [(bid, p, q) for bid, side in ((True, "bids"), (False, "asks"))
                       for p, q in message[side]]
            yield asset, "book", (True, changes, None)
        elif kind == "l2update":
            changes = [(side == "buy", p, q) for side, p, q in message["changes"]]
            yield asset, "book", (False, changes, None)
        elif kind == "heartbeat":
            yield asset, "heartbeat", int(message["last_trade_id"])
        elif kind in ("match", "last_match"):
            yield asset, kind, int(message["trade_id"])
        return

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
