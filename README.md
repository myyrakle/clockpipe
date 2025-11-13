# clockpipe

![](https://img.shields.io/badge/language-Rust-red) ![](https://img.shields.io/badge/version-0.5.4-brightgreen) [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/myyrakle/clockpipe/blob/master/LICENSE)

- Data synchronization pipeline tool for self-host clickhouse users.
- Automatically records data from the original source into Clickhouse, implemented through CDC.

## Supported Source

- PostgreSQL (ready)
- MongoDB (ready)
- MySQL (not yet)
- CassandraDB (not yet)
- ScyllaDB (not yet)

## Install

Build from source code

```bash
git clone https://github.com/myyrakle/clockpipe
cd clockpipe
cargo install --path .
```

Using Cargo

```bash
cargo install clockpipe
```

Using Docker

```bash
sudo docker run -v $(pwd)/clockpipe-config.json:/app/config.json --network host myyrakle/clockpipe:v0.5.4
```

## Requirements & Limits

- Each source has its own set of prerequisites and limitations.
- Please refer to the respective documentation.

1. [PostgreSQL](./docs/postgres/README.md)
2. [MongoDB](./docs/mongodb/README.md)

## How to Run

- Prepare config file. ([See documentation](./docs/README.md))
- Enter the information about the source table you want to synchronize. (postgres example)

```json
    "tables": [
        {
            "schema_name": "public",
            "table_name": "foo"
        },
        {
            "schema_name": "public",
            "table_name": "user_table"
        }
    ]
```

- Then, Run it

```bash
clockpipe run --config-file ./clockpipe-config.json
```

- Pipe automatically creates and synchronizes tables in Clickhouse by querying table information.

- If you don't want the initial synchronization, use the skip_copy option. (CDC-based synchronization still works.)

```json
    "tables": [
        {
            "schema_name": "public",
            "table_name": "user_table",
            "skip_copy": true
        }
    ]
```

## ETC

- You can also adjust the log level. You can set values such as error, warn, info, and debug to the "RUST_LOG" environment variable.

```
RUST_LOG=debug clockpipe run --config-file ./clockpipe-config.json
```
