# PostgreSQL Guideline

- CDC in PostgreSQL is implemented through [Publication](https://www.postgresql.org/docs/current/logical-replication-publication.html).

## PostgreSQL Setup

Synchronization is implemented through PostgreSQL Publication. You need to enable logical replication by modifying `postgresql.conf` and restarting the PostgreSQL server.

To find the config file location:

```bash
postgres=# SHOW config_file;
                   config_file
-------------------------------------------------
 /opt/homebrew/var/postgresql@14/postgresql.conf
```

Open the config file and set `wal_level` to `logical`:

```bash
sudo vim /opt/homebrew/var/postgresql@14/postgresql.conf
```

```
wal_level = logical
```

You may also want to adjust WAL retention options depending on your needs:

```
max_slot_wal_keep_size = -1   # unlimited retention (risk: disk bloat)
max_wal_size = 10240          # max WAL size in MB
```

Using `max_slot_wal_keep_size = -1` prevents data loss when replication falls behind, but can lead to significant disk usage if replication is not properly advanced.

Restart the database for the changes to take effect:

```bash
sudo systemctl restart postgresql
```

### Replica Identity

If a table contains columns with large values (over ~2KB), those values may not be included in the CDC log when they are not modified in an UPDATE statement. In this case, ClickHouse will display an empty value for those columns.

To ensure all column values are always included in replication, set the replica identity for the table to `FULL`:

```sql
ALTER TABLE [table_name] REPLICA IDENTITY FULL;
```

## Etc

- Columns added to the source table will also be automatically synchronized after the initial table link (requires restart).
- If a column is deleted from the source table, its values will be inserted as default values.
- If the accumulated WAL exceeds `max_slot_wal_keep_size`, `wal_status=lost` may occur and the CDC connection may be disconnected. In this case, you will need to remove and recreate the replication slot, which will result in losing any previously accumulated CDC logs.

## Caution

- If a column is dropped from an existing table during synchronization, problems may occur. This is because pgoutput will contain data from that point in time without any information about the dropped column.
- If `max_slot_wal_keep_size` is set to `-1`, be careful about disk bloat. If replication is not properly advanced, WAL data will continue to accumulate on disk.

---

## PostgreSQL Config

```json
    "postgres": {
      "publication_name": "clockpipe_publication",
      "replication_slot_name": "clockpipe_slot",
      "connection": {
        "host": "localhost",
        "port": 5432,
        "username": "your_user",
        "password": "your_password",
        "database": "your_database",
        "ssl_mode": "require",
        "ssl_root_cert": "/path/to/ca-certificate.crt"
      },
      "tables": [
        {
          "schema_name": "public",
          "table_name": "user_table",
          "mask_columns": ["password"]
        },
        {
          "schema_name": "public",
          "table_name": "order_table",
          "skip_copy": true
        }
      ]
    }
```

| name                   | description                                                        | required | default               |
| :--------------------- | :----------------------------------------------------------------- | :------- | :-------------------- |
| publication_name       | Publication name to use for CDC                                    | false    | clockpipe_publication |
| replication_slot_name  | Replication slot name to use for CDC                               | false    | clockpipe_slot        |
| connection             | PostgreSQL Database Connection Info                                | true     |                       |
| tables                 | Tables to sync                                                     | true     |                       |
| tables[].table_options | Table options. [Details](./../clickhouse/README.md)                | false    |                       |
| tables[].schema_name   | Schema name                                                        | true     |                       |
| tables[].table_name    | Table name                                                         | true     |                       |
| tables[].mask_columns  | Masks the values of specific columns to default values             | false    |                       |
| tables[].skip_copy     | Skip the initial full copy during first synchronization (CDC only) | false    | false                 |
