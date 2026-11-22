"""DepthScope service commands."""

import argparse
import logging
import json
from datetime import datetime

from . import services


def utc_second(value):
    try:
        value = datetime.fromisoformat(value)
        if value.utcoffset() is None or value.microsecond:
            raise ValueError
        return int(value.timestamp())
    except ValueError as exc:
        raise argparse.ArgumentTypeError("use a whole-second timestamp with a timezone, e.g. 2026-11-12T12:00:00Z") from exc


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(prog="depthscope")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("collector", "process", "archiver", "store-frames"):
        commands.add_parser(name)
    fees = commands.add_parser("fees")
    fees.add_argument("--path", required=True)
    fees.add_argument("--at", required=True, type=utc_second)
    replay = commands.add_parser("replay")
    replay.add_argument("--run-id", required=True)
    replay.add_argument("--start", required=True, type=utc_second)
    replay.add_argument("--end", required=True, type=utc_second)
    args = parser.parse_args()
    if args.command == "fees":
        from .fees import FeeSchedule
        print(json.dumps(FeeSchedule(args.path).at(args.at), sort_keys=True))
    elif args.command == "replay":
        if args.start > args.end:
            parser.error("replay requires start <= end")
        services.replay(args)
    elif args.command == "store-frames":
        services.persist(services.broker.FRAMES, "frames-store.pkl")
    else:
        getattr(services, args.command)()


if __name__ == "__main__":
    main()
