# PostgreSQL Guideline

- CDC in PostgreSQL is implemented through [Publication](https://www.postgresql.org/docs/current/logical-replication-publication.html).

## PostgreSQL Setup

- Synchronization is implemented through PostgreSQL Publication.
- modify `postgresql.conf` and restart postgresql server.

```bash
postgres=# SHOW config_file;
                   config_file
-------------------------------------------------
 /opt/homebrew/var/postgresql@14/postgresql.conf
```

enable logical replica, adjust

```bash
sudo vim /opt/homebrew/var/postgresql@14/postgresql.conf
```

```
wal_level=logical
```

You may also need to adjust the wal options to suit your needs.

```
wal_level=logical

max_slot_wal_keep_size=-1
max_wal_size=10240
```

If you want to minimize data loss, you should use `max_slot_wal_keep_size=-1`, but at the cost of potentially massive disk usage.

You need to restart the DB for the settings to take effect.

```bash
sudo systemctl restart postgresql
```

## Etc

- Columns added from the source will also be automatically synchronized after the initial table link. (if restarted)
- If a column is suddenly deleted, the values ​​in that column are inserted as default values.
- If the accumulated WAL exceeds `max_slot_wal_keep_size`, `wal_status=lost` may occur and the CDC connection may be disconnected. In this case, you will need to remove and recreate the replication_slot, which will result in losing any previously accumulated CDC logs.

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
        "database": "your_database"
      },
      "tables": [
        {
          "schema_name": "public",
          "table_name": "user_table",
          "mask_columns": ["password"]
        },
        {
          "schema_name": "public",
          "table_name": "user_table",
          "skip_copy": true
        }
      ]
    }
```

| name                  | description                                                   | required | default               |
| :-------------------- | :------------------------------------------------------------ | :------- | :-------------------- |
| publication_name      | Publication name to use for CDC                               | false    | clockpipe_publication |
| replication_slot_name | Replication slot name to use for CDC                          | false    | clockpipe_slot        |
| connection            | PostgreSQL Database Connection Info                           | true     |                       |
| tables                | tables to sync                                                | true     |                       |
| tables[].schema_name  | schema name                                                   | true     |                       |
| tables[].table_name   | table name                                                    | true     |                       |
| tables[].mask_columns | Masks the values ​​of specific columns to default values      | false    |                       |
| tables[].skip_copy    | Skip the first copy during initial synchronization (CDC only) | false    | false                 |
