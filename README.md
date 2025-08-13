# clockpipe

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
- Then, Run

```bash
clockpipe run --config-file ./clockpipe-config.json
```
