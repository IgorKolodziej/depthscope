"""Compose services for acquisition, storage and historical reprocessing."""

import asyncio
import logging
import os
import re
import time
from collections import Counter
from threading import Event, Thread

from . import broker, storage
from .archive import Archive
from .collector import capture
from .feeds import NS, VENUES
from .loadguard import LoadGuard, Overload
from .pipeline import Processor

RESYNC_STATUSES = {"invalid", "stale", "trade_gap", "clock_gap"}


def collector():
    broker.ensure_topics(broker.RAW, broker.RESYNC, broker.PROGRESS)
    writer = broker.Writer()
    resync = broker.Reader(broker.RESYNC, tail=True)
    health = broker.Reader(broker.PROGRESS, tail=True)
    guard = LoadGuard(int(os.getenv("MAX_PIPELINE_LAG_SECONDS", "30")))
    counts, sizes = Counter(), Counter()
    last_sample = last_receipt = None

    def publish(event):
        nonlocal last_sample, last_receipt
        last_receipt = event["receipt_ns"]
        writer.send(broker.RAW, event)
        if event["kind"] == "message":
            counts[event["venue"]] += 1
            sizes[event["venue"]] += len(event["payload"].encode())
        if event["kind"] != "tick":
            return
        writer.flush()
        second = event["receipt_ns"] // NS
        if last_sample is None:
            last_sample = second
            guard.sample(second)
        if second - last_sample < 10:
            return
        elapsed = second - last_sample
        rates = {venue: round(count / elapsed, 1) for venue, count in counts.items()}
        bytes_per_second = {venue: round(size / elapsed) for venue, size in sizes.items()}
        while item := health.poll(0):
            row = item[1]
            guard.progress(row["stage"], row["second"])
        try:
            lag = guard.sample(second)
        except Overload as exc:
            raise Overload(f"{exc}; latest messages/s={rates}; bytes/s={bytes_per_second}") from exc
        logging.info("messages/s=%s bytes/s=%s pipeline lag (s)=%s", rates, bytes_per_second, lag)
        counts.clear()
        sizes.clear()
        last_sample = second

    def requests():
        for _ in range(32):
            record = resync.poll(0)
            if record is None:
                break
            yield record[1]["venue"]

    try:
        asyncio.run(capture(publish, requests))
    except (Overload, RuntimeError) as exc:
        logging.error("Capture stopped: %s", exc)
        if last_receipt is not None:
            try:
                for venue in ("coinbase", "kraken"):
                    writer.send(broker.RAW, dict(kind="disconnected", receipt_ns=last_receipt,
                                                 venue=venue, payload=""))
                writer.flush()
            except RuntimeError:
                logging.exception("Could not publish feed shutdown")
        raise SystemExit(2) from exc
    finally:
        resync.close()
        health.close()


def process():
    stop = Event()

    def write_raw():
        while not stop.is_set():
            try:
                store_raw(stop)
            except Exception:
                logging.exception("ClickHouse raw-event writer will retry")
                stop.wait(2)

    raw = Thread(target=write_raw, name="raw-clickhouse")
    raw.start()
    try:
        process_frames(stop)
    finally:
        stop.set()
        raw.join()


def store_raw(stop):
    broker.ensure_topics(broker.RAW, broker.PROGRESS)
    storage.initialize_raw()
    state = storage.load_state("raw-store.pkl")
    reader = broker.Reader(broker.RAW, state["next_offset"] if state else 0)
    writer = broker.Writer()
    batch = []
    deadline = time.monotonic() + 3

    def flush():
        offset, last = batch[-1]
        storage.insert_raw(batch)
        storage.save_state("raw-store.pkl", dict(next_offset=offset + 1))
        writer.send(broker.PROGRESS, dict(stage="raw_clickhouse", second=last["receipt_ns"] // NS))
        writer.flush()
        batch.clear()

    try:
        while not stop.is_set():
            now = time.monotonic()
            if now >= deadline:
                if batch:
                    flush()
                deadline = time.monotonic() + 3
            item = reader.poll(min(0.25, max(0, deadline - time.monotonic())))
            if item is not None:
                batch.append(item)
                if len(batch) >= 5000:
                    flush()
                    deadline = time.monotonic() + 3
        if batch:
            flush()
    finally:
        reader.close()


def process_frames(stop):
    broker.ensure_topics(broker.RAW, broker.FRAMES, broker.RESYNC, broker.PROGRESS, broker.CHECKPOINTS)
    writer = broker.Writer()
    processor = Processor()
    reader = broker.Reader(broker.RAW, tail=True)
    last_resync = {}
    try:
        for venue in VENUES:
            writer.send(broker.RESYNC, dict(venue=venue, second=time.time_ns() // NS))
        writer.flush()
        while not stop.is_set():
            item = reader.poll()
            if item is None:
                continue
            offset, event = item
            frames = processor.process(event)
            if frames is None:
                continue
            second = processor.second
            writer.send(broker.FRAMES, dict(second=second, frames=frame_rows(second, frames)))
            for (venue, _), frame in frames.items():
                if frame["status"] in RESYNC_STATUSES and second >= last_resync.get(venue, -30) + 15:
                    writer.send(broker.RESYNC, dict(venue=venue, second=second))
                    last_resync[venue] = second
            if second % 5 == 0:
                writer.send(broker.PROGRESS, dict(stage="processor", second=second))
            if second % 300 == 0:
                writer.send(broker.CHECKPOINTS,
                            dict(next_offset=offset + 1, processor=processor.checkpoint()))
                writer.flush()
    finally:
        reader.close()
        processor.close()


def frame_rows(second, frames):
    return [dict(frame, second=second, venue=venue, asset=asset)
            for (venue, asset), frame in frames.items()]


def persist(topic, state_file):
    broker.ensure_topics(topic, broker.PROGRESS)
    writer = broker.Writer()
    storage.initialize()
    saved = storage.load_state(state_file)
    reader = broker.Reader(topic, saved["next_offset"] if saved else 0)
    try:
        while True:
            item = reader.poll()
            if item is None:
                continue
            batches = []
            while item is not None and len(batches) < 20:
                batches.append(item)
                if len(batches) < 20:
                    item = reader.poll(0.1)
            rows = {}
            for offset, batch in batches:
                for kind in ("frames",):
                    rows.setdefault(kind, []).extend(batch.get(kind, ()))
            for kind, records in rows.items():
                storage.insert("live", kind, records)
            storage.save_state(state_file, dict(next_offset=offset + 1))
            if topic == broker.FRAMES:
                writer.send(broker.PROGRESS, dict(stage="frames", second=batch["second"]))
    finally:
        reader.close()


def archiver():
    broker.ensure_topics(broker.RAW, broker.PROGRESS, broker.CHECKPOINTS)
    writer = broker.Writer()
    saved = storage.load_state("archive.pkl")
    archive = Archive()
    reader = broker.Reader(broker.RAW, saved["next_offset"] if saved else 0)
    state = storage.load_state("checkpoint-archive.pkl")
    checkpoints = broker.Reader(broker.CHECKPOINTS, state["next_offset"] if state else 0)
    batch = []
    deadline = time.monotonic() + 10
    try:
        while True:
            if checkpoint := checkpoints.poll(0):
                checkpoint_offset, row = checkpoint
                archive.save_checkpoint(row["next_offset"], row["processor"])
                storage.save_state("checkpoint-archive.pkl", dict(next_offset=checkpoint_offset + 1))
            item = reader.poll(0.5)
            if item:
                batch.append(item)
            if batch and (len(batch) >= 5000 or time.monotonic() >= deadline):
                archive.write_events(batch)
                storage.save_state("archive.pkl", dict(next_offset=batch[-1][0] + 1))
                writer.send(broker.PROGRESS, dict(stage="archive", second=batch[-1][1]["receipt_ns"] // NS))
                batch.clear()
                deadline = time.monotonic() + 10
    finally:
        reader.close()
        checkpoints.close()



def replay(args):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.run_id) or args.run_id == "live":
        raise ValueError("run ID must be 1–64 letters, digits, underscores or hyphens and cannot be live")
    topic = f"depthscope.replay.{args.run_id}"
    broker.ensure_topics(topic, exclusive=True)
    reader = None
    writer = broker.Writer()
    archive = Archive()
    storage.initialize()
    processor = Processor()
    pending = []
    next_second = args.start

    def consume():
        nonlocal reader, next_second
        if reader is None:
            reader = broker.Reader(topic, 0)
        item = reader.poll(30)
        if item is None:
            raise RuntimeError("timed out waiting for replay events")
        _, event = item
        frames = processor.process(event)
        if frames is None:
            return
        second = processor.second
        if args.start <= second <= args.end:
            if second != next_second:
                raise RuntimeError(f"missing archived clock tick at {next_second}")
            next_second += 1
            pending.extend(frame_rows(second, frames))
        if second % 10 == 0:
            storage.insert(args.run_id, "frames", pending)
            pending.clear()

    started = time.monotonic()
    total = 0
    try:
        checkpoint = archive.checkpoint(args.start - 1)
        if checkpoint:
            processor.restore(checkpoint["processor"])
        source = archive.events(args.end * NS, checkpoint["next_offset"] if checkpoint else None)
        count = 0
        for event in source:
            writer.send(topic, event)
            total += 1
            count += 1
            if count == 256:
                writer.flush()
                for _ in range(count):
                    consume()
                count = 0
        writer.flush()
        for _ in range(count):
            consume()
        if next_second != args.end + 1:
            raise RuntimeError(f"missing archived clock tick at {next_second}")
        storage.insert(args.run_id, "frames", pending)
        storage.insert(args.run_id, "run", [dict(second=args.start, end=args.end, asset="",
                                                 archive_events=total, schema=3)])
        elapsed = time.monotonic() - started
        logging.info("Replay %s complete: %d events in %.1fs (%.0f events/s)",
                     args.run_id, total, elapsed, total / elapsed)
    finally:
        processor.close()
        if reader is not None:
            reader.close()
