"""Read scenario-specific, versioned maker and taker fee inputs."""

import csv
from bisect import bisect_right
from datetime import datetime
from math import isfinite

from .feeds import VENUES


class FeeSchedule:
    def __init__(self, path):
        self.rows = {venue: [] for venue in VENUES}
        with open(path, newline="", encoding="utf-8") as stream:
            for row in csv.DictReader(stream):
                stamp = datetime.fromisoformat(row["effective_utc"])
                if stamp.utcoffset() is None or stamp.microsecond:
                    raise ValueError("effective_utc must be a whole-second timestamp with a timezone")
                taker, maker = float(row["taker_bps"]), float(row["maker_bps"])
                if not isfinite(taker) or taker < 0 or not isfinite(maker):
                    raise ValueError("invalid fee in basis points")
                self.rows[row["venue"]].append((int(stamp.timestamp()), taker))
        for venue, rows in self.rows.items():
            rows.sort()
            if not rows or len({stamp for stamp, _ in rows}) != len(rows):
                raise ValueError(f"missing or duplicate fee effective date for {venue}")

    def at(self, second):
        fees = {}
        for venue, rows in self.rows.items():
            index = bisect_right(rows, (second, float("inf"))) - 1
            if index < 0:
                raise ValueError(f"fee schedule does not cover timestamp {second} on {venue}")
            fees[venue] = rows[index][1]
        return fees
