# Fee input

`fees.csv` records the standard entry-level maker/taker rates observed on the official pages on 21 November 2026. Values are basis points (100 bps = 1%). The first effective date is an observation-date scenario boundary, rather than an assertion about when the exchange changed its fees. Use these rows for data-loading demonstration and verify the applicable account tier before later execution experiments.

Coinbase Exchange lists 40 bps maker and 60 bps taker for the $0K-$10K volume tier. Kraken Pro lists 40 bps maker and 80 bps taker for Tier 1 standard spot. Each row retains its source URL. Higher-volume tiers, pair discounts, regional and negotiated fees can differ.

The loader validates all rows, rejects non-finite values or missing venue coverage, and selects the latest effective taker rate at the requested time. Maker rates remain in the source CSV for later modelling.

Load the batch input inside Compose with:

```sh
docker compose run --rm --no-deps -v "$PWD/data:/data:ro" processor fees --path /data/fees.csv --at 2026-11-21T12:00:00Z
```
