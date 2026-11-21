# DepthScope

Data acquisition and pre-processing for BTC/USD and ETH/USD on Coinbase Exchange and Kraken. This project covers the **Data milestone**. The C engine reconstructs and validates order books for checksum and source-quality checks. Python manages public WebSocket connections, Kafka transport and storage.

## Run the Data milestone

```sh
docker compose build processor
docker compose up -d --no-build
docker compose logs -f collector
```

All services run in Docker Compose. The project name is `depthscope-data`. The stack contains Kafka, ClickHouse, SeaweedFS object storage, collector, processor, archiver and frame storage. The image is built once by the processor service and shared by the other application services.

The collector logs source message rates, payload bytes and downstream lag every ten seconds. It stops capture if a monitored stage stays above the configured lag limit after startup, or the capture queue delay exceeds ten seconds. Inspect collector logs when capture stops. Stop the stack with `docker compose down`; named volumes retain the recorded data.

## Data and validation

`depthscope/book.c` reconstructs the source books for ordering and checksum validation. The processor's main thread applies events and publishes frames at each recorded one-second clock tick. Its second thread batches the raw Kafka messages into ClickHouse every three seconds or after 5,000 messages. The independent archiver writes the complete raw stream to S3-compatible storage.

The one-second records contain source book levels, acquisition status and instrument increments. Incremental quantities replace earlier values and zero deletes a level. Kraken checksums and Coinbase heartbeat checks support consistency monitoring. Decimal source prices and quantities are converted to exact units of 10^-8 for C state. Liquidity metrics, rolling features, forecasting, execution experiments and the dashboard are assigned to subsequent milestones.

[Fee CSV and loader instructions](data/README.md) describe the batch input. The [ownership table](docs/ownership.md) maps Data responsibilities to the team members.

```sh
make test
ruff check depthscope tests scripts
docker compose config --quiet
```

The local test command needs Python 3.12, PyArrow, a C compiler and zlib headers. Runtime dependencies are installed inside Docker. After at least 135 seconds of capture, `python3 scripts/verify_data.py` records the functional tests, current live rates and frame continuity in the milestone evidence directory. The verifier needs Docker access and local Ruff/PyArrow. Raw messages are archived as Zstandard Parquet batches with offset manifests; a historical replay reader is planned for later work.

## Milestone submission

The [Data milestone](milestones/data/README.md) contains the report, presentation, test documentation, LaTeX sources and captured evidence. The report identifies implemented modules, planned regression and visualization work, and known data-quality limitations.

[Source repository](https://github.com/IgorKolodziej/depthscope)
