# clockpipe

![](https://img.shields.io/badge/language-Rust-red) ![](https://img.shields.io/badge/version-0.4.1-brightgreen) [![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/myyrakle/clockpipe/blob/master/LICENSE)

- An alternative to clickpipe for on-premise clickhouse users.
- Based on CDC, data from the original source is written to clickhouse.

## Supported Source

- PostgreSQL

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
sudo docker run -v $(pwd)/clockpipe-config.json:/app/config.json --network host myyrakle/clockpipe:v0.4.1
```

## PostgreSQL Setup

- Synchronization is implemented through PostgreSQL Publication.
- modify `postgresql.conf` and restart postgresql server.

```bash
postgres=# SHOW config_file;
                   config_file
-------------------------------------------------
 /opt/homebrew/var/postgresql@14/postgresql.conf
```

enable logical replica

```bash
sudo vim /opt/homebrew/var/postgresql@14/postgresql.conf
```

```
wal_level=logical

max_slot_wal_keep_size=-1
max_wal_size=10240
```

```bash
sudo systemctl restart postgresql
```

## How to Run

- Prepare config file ([example](./example.json))
- Enter the information about the PostgreSQL table you want to synchronize.

```json
    "tables": [
        {
            "schema_name": "public",
            "table_name": "foo"
        },
        {
            "schema_name": "public",
            "table_name": "nc_usr_account"
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

- You can also adjust the log level. You can set values such as error, warn, info, and debug to the "RUST_LOG" environment variable.

```
RUST_LOG=debug clockpipe run --config-file ./clockpipe-config.json
```

- Columns added from the source will also be automatically synchronized after the initial table link. (if restarted)
