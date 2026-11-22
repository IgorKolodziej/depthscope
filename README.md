# DepthScope

We collect BTC/USD and ETH/USD order book and trade data from Coinbase and Kraken. This checkpoint covers collection, validation and storage in ClickHouse and Parquet.

## Run

```sh
docker compose build processor
docker compose up -d --no-build
docker compose logs -f collector
```

Use `docker compose down` to stop the services. Run `make test` for the local tests.

## Replay

Pick a UTC interval from your capture and use a fresh run ID:

```sh
docker compose --profile research run --rm replay \
  --run-id demo-01 --start 2026-11-22T18:58:06Z --end 2026-11-22T19:00:05Z
```

Replay reads the Parquet archive and writes the rebuilt books to ClickHouse under the run ID. Run `python3 scripts/verify_data.py` after at least three minutes of capture to compare replayed and live records.

The report, slides and test results are in [milestones/data](milestones/data/README.md).
