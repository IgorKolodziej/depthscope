"""Check a Kraken snapshot using its ten best asks and bids."""

from decimal import Decimal
from zlib import crc32


def checksum_digits(value):
    return format(Decimal(value), "f").replace(".", "").lstrip("0")


def verify_kraken_snapshot(snapshot):
    parts = []
    for side in ("asks", "bids"):
        levels = sorted(
            snapshot[side], key=lambda level: Decimal(level["price"]), reverse=side == "bids"
        )
        for level in levels[:10]:
            parts.extend(checksum_digits(level[field]) for field in ("price", "qty"))
    if crc32("".join(parts).encode("ascii")) != int(snapshot["checksum"]):
        raise ValueError("Kraken book checksum mismatch")
