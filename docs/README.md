## Config File Format

config example

```json
{
  "source": {
    "source_type": "postgres",
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
  },
  "target": {
    "target_type": "clickhouse",
    "clickhouse": {
      "connection": {
        "host": "localhost",
        "port": 8123,
        "username": "your_user",
        "password": "your_password",
        "database": "your_database"
      },
      "table_options": null
    }
  },
  "sleep_millis_when_peek_failed": 5000,
  "sleep_millis_when_peek_is_empty": 5000,
  "sleep_millis_when_write_failed": 5000,
  "sleep_millis_after_sync_iteration": 100,
  "sleep_millis_after_sync_write": 100,
  "peek_changes_limit": 65536
}
```

| name                                | description                                                                  | required | default |
| :---------------------------------- | :--------------------------------------------------------------------------- | :------- | :------ |
| source                              | Defines which source to retrieve data from.                                  | true     |         |
| source.source_type                  | Type of data source (DB type)                                                | true     |         |
| source.postgres                     | See [The PostgreSQL Document](./postgres/README.md)                          | -        |         |
| source.mongodb                      | See [The MongoDB Document](./mongodb/README.md)                              | -        |         |
| target                              | Where to store your data (by default clickhouse)                             | true     |         |
| target.target_type                  | clickhouse                                                                   | true     |         |
| target.clickhouse.table_options     | global table options. [Details](./clickhouse/README.md)                      | false    |         |
| target.clickhouse.disable_sync_loop | Disables continuous synchronization. Only the first copy is processed.       | false    | false   |
| sleep_millis_when_peek_failed       | Wait time when fetching CDC data fails. (ms)                                 | false    | 5000    |
| sleep_millis_when_peek_is_empty     | Wait time when there are no results from retrieving CDC data. (ms)           | false    | 5000    |
| sleep_millis_when_write_failed      | Wait time when writing using CDC data fails (ms)                             | false    | 5000    |
| sleep_millis_after_sync_iteration   | Wait time per iteration of the CDC loop (ms)                                 | false    | 100     |
| sleep_millis_after_sync_write       | Wait time after writing using CDC data (ms)                                  | false    | 100     |
| peek_changes_limit                  | Maximum number of data to retrieve per CDC iteration                         | false    | 65536   |
| copy_batch_size                     | When copy inserting in clickhouse, the number of rows included in one insert | false    | 100000  |
