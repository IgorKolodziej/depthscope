# Data milestone responsibilities

| Team member | Data component | Primary files |
| --- | --- | --- |
| Igor Kołodziej | Kraken collector and checksum verification | `depthscope/feeds.py`, `depthscope/book.c`, `depthscope/book.py`, shared collector integration |
| Adam Kaniasty | Kafka, ClickHouse schemas, Parquet archive and deployment | `depthscope/broker.py`, `depthscope/storage.py`, `depthscope/archive.py`, `depthscope/services.py`, `compose.yaml`, Docker build |
| Mateusz Iwaniuk | Coinbase collector and heartbeat gap detection | `depthscope/collector.py`, `depthscope/pipeline.py`, shared feed adapters |
| Hubert Kowalski | Fee schedule batch source and loading | `depthscope/fees.py`, fee CSV input and fee tests |

Collectors and adapters share common transport and normalization code. The table follows the responsibility split in the project plan.

## Milestone scope

This milestone covers source acquisition, checksum and heartbeat validation, raw/normalized storage, fee loading and archiving. Historical replay, recovery experiments, liquidity metrics, rolling features, regression, execution-policy experiments and the dashboard are planned for subsequent stages.

## Data code boundaries

The C code applies source snapshots and absolute level changes, truncates Kraken books to the subscribed depth, verifies CRC32 and checks book ordering. Its Python binding converts decimal source values and maintains the current acquisition book. The acquisition pipeline handles connection status, heartbeat checks and instrument metadata.

ClickHouse stores original events and normalized acquisition records keyed by run, venue, asset and time. The fee module loads the versioned batch input. The archive writer stores original messages and per-batch manifests. A historical replay reader is planned for the Processing milestone.
