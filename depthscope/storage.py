"""ClickHouse storage and atomic local service checkpoints."""

import json
import os
import pickle
import tempfile
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

STATE = Path(os.environ.get("DEPTHSCOPE_STATE", "/state"))


def load_state(name):
    path = STATE / name
    if path.exists():
        with path.open("rb") as stream:
            return pickle.load(stream)
    return None


def save_state(name, state):
    STATE.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=STATE, delete=False) as stream:
        temporary = Path(stream.name)
        try:
            pickle.dump(state, stream)
            stream.flush()
            os.fsync(stream.fileno())
            temporary.replace(STATE / name)
            directory = os.open(STATE, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)


def query(sql, data=None, parameters=None):
    params = {"query": sql}
    params.update({f"param_{key}": value for key, value in (parameters or {}).items()})
    base = os.environ.get("CLICKHOUSE_URL", "http://clickhouse:8123")
    headers = {"X-ClickHouse-User": os.getenv("CLICKHOUSE_USER", "depthscope"),
               "X-ClickHouse-Key": os.getenv("CLICKHOUSE_PASSWORD", "depthscope-local")}
    request = Request(f"{base}/?{urlencode(params)}", data=data, headers=headers)
    try:
        with urlopen(request, timeout=20) as response:
            return response.read()
    except HTTPError as exc:
        detail = exc.read().decode(errors="replace").strip()
        raise HTTPError(exc.url, exc.code, detail, exc.headers, None) from exc


def initialize():
    query("""
        CREATE TABLE IF NOT EXISTS observations (
            run LowCardinality(String), kind LowCardinality(String), second Int64,
            venue LowCardinality(String), asset LowCardinality(String), payload String
        ) ENGINE = ReplacingMergeTree
        ORDER BY (run, kind, asset, venue, second)
    """, b"")


def insert(run, kind, rows):
    if not rows:
        return
    records = []
    for row in rows:
        records.append(json.dumps(dict(
            run=run, kind=kind, second=row["second"],
            venue=row.get("venue", ""), asset=row.get("asset", ""),
            payload=json.dumps(row, allow_nan=False, separators=(",", ":")),
        ), allow_nan=False))
    query("INSERT INTO observations FORMAT JSONEachRow", ("\n".join(records) + "\n").encode())



def initialize_raw():
    query("""
        CREATE TABLE IF NOT EXISTS raw_events (
            offset UInt64,
            receipt_ns Int64 CODEC(Delta, ZSTD(3)),
            received_ns Int64 CODEC(Delta, ZSTD(3)),
            venue LowCardinality(String),
            kind LowCardinality(String),
            payload String CODEC(ZSTD(3))
        ) ENGINE = ReplacingMergeTree
        PARTITION BY toYYYYMM(fromUnixTimestamp64Nano(receipt_ns))
        ORDER BY offset
    """, b"")


def insert_raw(batch):
    records = []
    for offset, row in batch:
        records.append(json.dumps(dict(
            offset=offset, receipt_ns=row["receipt_ns"],
            received_ns=row.get("received_ns", row["receipt_ns"]),
            venue=row["venue"], kind=row["kind"], payload=row["payload"],
        ), separators=(",", ":")))
    query("INSERT INTO raw_events FORMAT JSONEachRow", ("\n".join(records) + "\n").encode())
