"""Receipt-time normalization and validation of live acquisition events."""

from .book import SCALE, Book, fixed, load_library
from .feeds import ASSETS, NS, VENUES, decode

FEED_TIMEOUT = 10


class Market:
    def __init__(self, library, venue):
        self.book = Book(library, 1000 if venue == "kraken" else 0)
        self.instrument = None
        self.reset("initializing")

    def reset(self, status):
        self.book.invalidate(status)
        self.last_seen = None
        self.last_trade = None
        self.expected_trade = None

    def apply(self, second, kind, data):
        if kind == "instrument":
            status, price_step, quantity_step = data
            price_step, quantity_step = fixed(price_step), fixed(quantity_step)
            if not price_step or not quantity_step:
                raise ValueError("instrument increment must be positive")
            previous = self.instrument
            self.instrument = (status, price_step, quantity_step)
            if previous and previous[1:] != self.instrument[1:]:
                self.reset("invalid")
            return
        self.last_seen = second
        if kind == "book":
            self.book.apply(*data)
            if self.book.ready and self.book.status != "valid":
                raise ValueError("locked, crossed or one-sided book")
        elif kind in ("match", "last_match"):
            trade_id = data
            if self.last_trade is None or trade_id > self.last_trade:
                self.last_trade = trade_id
        elif kind == "heartbeat" and data is not None:
            if self.last_trade is not None and data > self.last_trade and self.expected_trade is None:
                self.expected_trade = (second, data)

    def frame(self, second):
        if self.last_seen is not None and second - self.last_seen >= FEED_TIMEOUT:
            self.reset("stale")
        if self.expected_trade:
            since, trade_id = self.expected_trade
            if self.last_trade >= trade_id:
                self.expected_trade = None
            elif second - since >= 3:
                self.reset("trade_gap")
        usable = self.book.valid()
        status = self.book.status
        if self.instrument and self.instrument[0] != "online":
            usable = False
            status = f"market_{self.instrument[0]}"
        frame = dict(status=status, bids=[], asks=[], price_increment=None, quantity_increment=None)
        if self.instrument:
            frame.update(price_increment=self.instrument[1] / SCALE,
                         quantity_increment=self.instrument[2] / SCALE)
        if usable:
            frame["asks"] = self.book.levels(False)
            frame["bids"] = self.book.levels(True)
        return frame


class Processor:
    def __init__(self):
        library = load_library()
        self.markets = {(venue, asset): Market(library, venue) for venue in VENUES for asset in ASSETS}
        self.receipt_ns = -1
        self.second = None

    def close(self):
        for market in self.markets.values():
            market.book.close()

    def process(self, event):
        timestamp = event["receipt_ns"]
        if timestamp < self.receipt_ns:
            raise ValueError("events must be in receipt order")
        self.receipt_ns = timestamp
        kind = event["kind"]
        second = timestamp // NS
        if kind == "tick":
            if timestamp % NS or (self.second is not None and second <= self.second):
                raise ValueError("invalid or duplicate tick")
            if self.second is not None and second != self.second + 1:
                for market in self.markets.values():
                    market.reset("clock_gap")
            self.second = second
            return {key: market.frame(second) for key, market in self.markets.items()}

        venue = event["venue"]
        if kind in ("connected", "disconnected"):
            for asset in ASSETS:
                self.markets[(venue, asset)].reset("initializing" if kind == "connected" else "disconnected")
        elif kind == "message":
            try:
                for asset, decoded, data in decode(venue, event["payload"]):
                    market = self.markets[(venue, asset)]
                    try:
                        market.apply(second, decoded, data)
                    except (ArithmeticError, ValueError):
                        market.reset("invalid")
            except (KeyError, TypeError, ArithmeticError, ValueError):
                for asset in ASSETS:
                    self.markets[(venue, asset)].reset("invalid")
        else:
            raise ValueError(f"unknown event kind {kind}")
        return None
