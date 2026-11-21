import tempfile
import unittest
from pathlib import Path

from depthscope.fees import FeeSchedule


class DataInputTests(unittest.TestCase):
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

