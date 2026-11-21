import json
import tempfile
import unittest
from io import BytesIO
from pathlib import Path
from threading import Event
from unittest.mock import Mock, patch
from zlib import crc32

from depthscope import services
from depthscope.archive import Archive
from depthscope.book import Book, load_library
from depthscope.feeds import NS
from depthscope.fees import FeeSchedule
from depthscope.loadguard import LoadGuard, Overload
from depthscope.pipeline import Processor


def event(second, kind="tick", venue="", payload=None):
    return dict(receipt_ns=second * NS, kind=kind, venue=venue,
                payload=json.dumps(payload) if payload is not None else "")


def snapshot(second):
    return event(second, "message", "coinbase",
                 dict(type="snapshot", product_id="BTC-USD", bids=[["999.50", "2"]],
                      asks=[["1000.50", "1"], ["1001.00", "2"], ["1001.01", "20"]]))


class MemoryArchive(Archive):
    def __init__(self):
        self.objects = {}

    def put(self, key, data):
        self.objects[key] = data



class BookTests(unittest.TestCase):
    def setUp(self):
        self.processor = Processor()
        self.addCleanup(self.processor.close)

    def test_absolute_updates_and_trade_independence(self):
        self.processor.process(snapshot(0))
        self.processor.process(event(0, "message", "coinbase", dict(type="match", product_id="BTC-USD",
                               trade_id=7, price="1000.50", size="0.75")))
        frame = self.processor.process(event(1))[("coinbase", "BTC")]
        self.assertEqual(frame["asks"], [(1000.5, 1), (1001.0, 2), (1001.01, 20)])
        self.assertEqual(frame["bids"], [(999.5, 2)])
        self.processor.process(event(1, "message", "coinbase", dict(type="l2update",
                               product_id="BTC-USD", changes=[["sell", "1001.00", "0"],
                                                             ["sell", "1000.50", "0.25"]])))
        frame = self.processor.process(event(2))[("coinbase", "BTC")]
        self.assertEqual(frame["asks"], [(1000.5, 0.25), (1001.01, 20)])

    def test_gap_suppresses_book_until_new_snapshot(self):
        self.processor.process(snapshot(0))
        self.processor.process(event(1))
        self.processor.process(event(1, "disconnected", "coinbase"))
        self.processor.process(event(2, "message", "coinbase", dict(type="l2update",
                               product_id="BTC-USD", changes=[["sell", "1000.50", "3"]])))
        frame = self.processor.process(event(2))[("coinbase", "BTC")]
        self.assertEqual(frame["asks"], [])
        self.processor.process(snapshot(2))
        self.assertEqual(self.processor.process(event(3))[("coinbase", "BTC")]["status"], "valid")

    def test_kraken_checksum_truncation_and_bad_update(self):
        book = Book(load_library(), 2)
        self.addCleanup(book.close)
        changes = [(False, "1000.5", "2.0"), (False, "1000.6", "1.0"), (False, "1000.7", "1.0"),
                   (True, "999.5", "3.0"), (True, "998.0", "1.0")]
        checksum = crc32(b"10005201000610999530998010")
        book.apply(True, changes, checksum)
        self.assertTrue(book.valid())
        self.assertEqual(book.levels(False), [(1000.5, 2), (1000.6, 1)])
        self.assertEqual(book.levels(True), [(999.5, 3), (998.0, 1)])
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            book.apply(False, [(False, "1000.5", "9.0")], 0)
        self.assertFalse(book.valid())
        book.apply(False, changes, checksum)
        self.assertFalse(book.valid())

    def test_market_halt_suppresses_book(self):
        self.processor.process(snapshot(0))
        self.processor.process(event(61, "message", "coinbase", dict(type="status", products=[
            dict(id="BTC-USD", status="online", quote_increment="0.01",
                 base_increment="0.00000001", cancel_only=True)])))
        frame = self.processor.process(event(62))[("coinbase", "BTC")]
        self.assertEqual(frame["status"], "market_cancel_only")
        self.assertEqual(frame["asks"], [])

    def test_parquet_archive_preserves_raw_events(self):
        import pyarrow.parquet as pq

        archive = MemoryArchive()
        delta = event(1, "message", "coinbase", dict(
            type="l2update", product_id="BTC-USD", time="2026-11-21T20:00:00Z",
            changes=[["sell", "1000.50", "0.25"]],
        ))
        batch = [(10, snapshot(0)), (11, delta), (12, event(1))]
        archive.write_events(batch)
        manifest_key = next(reversed(archive.objects))
        self.assertTrue(manifest_key.startswith("index/"))
        manifest = json.loads(archive.objects[manifest_key])
        self.assertEqual((manifest["first"], manifest["last"]), (10, 12))
        self.assertEqual(manifest["start_ns"], 0)
        self.assertEqual(manifest["end_ns"], NS)
        rows = {}
        for path in manifest["paths"]:
            parquet = pq.ParquetFile(BytesIO(archive.objects[path]))
            self.assertEqual(parquet.metadata.row_group(0).column(0).compression, "ZSTD")
            for row in parquet.read().to_pylist():
                rows[row["offset"]] = row
        self.assertEqual(set(rows), {10, 11, 12})
        for offset, original in batch:
            for field, value in original.items():
                self.assertEqual(rows[offset][field], value)
        self.assertEqual(rows[11]["exchange_time"], "2026-11-21T20:00:00Z")
        self.assertEqual(rows[11]["channel"], "l2update")
        self.assertTrue(any("venue=coinbase/asset=BTC/" in path for path in manifest["paths"]))


class DataInputTests(unittest.TestCase):
    def test_acquisition_start_requests_fresh_source_books(self):
        stop = Event()
        reader, writer, processor = Mock(), Mock(), Mock()

        def end_capture():
            stop.set()
            return None

        reader.poll.side_effect = end_capture
        with (
            patch.object(services.broker, "ensure_topics"),
            patch.object(services.broker, "Writer", return_value=writer),
            patch.object(services.broker, "Reader", return_value=reader) as create_reader,
            patch.object(services, "Processor", return_value=processor),
        ):
            services.process_frames(stop)
        create_reader.assert_called_once_with(services.broker.RAW, tail=True)
        requested = [(call.args[0], call.args[1]["venue"]) for call in writer.send.call_args_list]
        self.assertEqual(requested, [(services.broker.RESYNC, "coinbase"), (services.broker.RESYNC, "kraken")])
        writer.flush.assert_called_once()
        processor.close.assert_called_once()
        reader.close.assert_called_once()

    def test_fee_schedule_switches_at_the_effective_second(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fees.csv"
            path.write_text("venue,effective_utc,maker_bps,taker_bps\n"
                            "coinbase,1970-01-01T00:00:00Z,2,6\n"
                            "coinbase,1970-01-01T00:01:00Z,2,5\n"
                            "kraken,1970-01-01T00:00:00Z,-1,4\n")
            fees = FeeSchedule(path)
        self.assertEqual(fees.at(59), {"coinbase": 6, "kraken": 4})
        self.assertEqual(fees.at(60), {"coinbase": 5, "kraken": 4})


    def test_stop_on_sustained_lag(self):
        guard = LoadGuard(limit=10, grace=20, consecutive=3)
        guard.sample(0)
        for second in (20, 30, 40):
            for stage in ("processor", "frames", "raw_clickhouse"):
                guard.progress(stage, second)
            if second == 40:
                with self.assertRaisesRegex(Overload, "archive lag"):
                    guard.sample(second)
            else:
                guard.sample(second)
