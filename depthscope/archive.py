"""Partitioned Parquet storage for original acquisition events."""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone
from io import BytesIO

from .feeds import ASSETS, NS


def market_metadata(event):
    if event["kind"] != "message":
        return "_all", "", "", ""
    try:
        message = json.loads(event["payload"])
    except (TypeError, ValueError):
        return "_all", "", "", ""
    if not isinstance(message, dict):
        return "_all", "", "", ""
    rows = message.get("data") or []
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        rows = []
    market = message.get("product_id") or (rows[0].get("symbol", "") if rows else "")
    if rows and any(row.get("symbol", market) != market for row in rows):
        market = ""
    asset = market.split("-")[0].split("/")[0]
    details = rows[0] if len(rows) == 1 else message
    channel = message.get("channel", message.get("type", ""))
    timestamp = details.get("timestamp", details.get("time", ""))
    marker = details.get("checksum", details.get("sequence", details.get("trade_id", "")))
    return asset if asset in ASSETS else "_all", str(channel or ""), str(timestamp or ""), str(marker)


class Archive:
    def __init__(self):
        import boto3
        from botocore.config import Config

        self.bucket = os.getenv("ARCHIVE_BUCKET", "depthscope")
        self.s3 = boto3.client("s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://objectstore:8333"),
                               region_name="us-east-1", config=Config(s3={"addressing_style": "path"}))

    def put(self, key, data):
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=data)

    def write_events(self, batch):
        import pyarrow as pa
        import pyarrow.parquet as pq

        groups = defaultdict(list)
        first, last = batch[0][0], batch[-1][0]
        for offset, event in batch:
            stamp = datetime.fromtimestamp(event["receipt_ns"] // NS, tz=timezone.utc)
            asset, channel, exchange_time, marker = market_metadata(event)
            partition = (f"venue={event['venue'] or '_clock'}/asset={asset}/"
                         f"date={stamp:%Y-%m-%d}/hour={stamp:%H}")
            groups[partition].append(dict(event, offset=offset, channel=channel,
                                          exchange_time=exchange_time, sequence_or_checksum=marker))
        paths = []
        for partition, rows in groups.items():
            path = f"raw/{partition}/{first:020d}-{last:020d}.parquet"
            buffer = BytesIO()
            pq.write_table(pa.Table.from_pylist(rows), buffer, compression="zstd")
            self.put(path, buffer.getvalue())
            paths.append(path)
        self.put(f"index/{first:020d}-{last:020d}.json", json.dumps(dict(
            first=first, last=last, start_ns=batch[0][1]["receipt_ns"],
            end_ns=batch[-1][1]["receipt_ns"], paths=paths,
        )).encode())
