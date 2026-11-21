# DepthScope

We collect BTC/USD and ETH/USD order book and trade data from Coinbase and Kraken. This checkpoint covers collection, validation and storage in ClickHouse and Parquet.

## Run

```sh
docker compose build processor
docker compose up -d --no-build
docker compose logs -f collector
```

Use `docker compose down` to stop the services. Run `make test` for the local tests.

The report, slides and test results are in [milestones/data](milestones/data/README.md).
