## Clickhouse Table Options

These are table-level options applied when creating tables in Clickhouse.

```json
{
  "table_options": {
    "storage_policy": "basic_storage",
    "granularity": 8192
  }
}
```

You can set these options in the global settings (target.clickhouse.table_options) or in the table-specific settings (source.postgres.tables[].table_options) of individual sources. If the table-specific settings are left blank, the global settings are inherited.

| name                           | description                             | required | default |
| :----------------------------- | :-------------------------------------- | :------- | :------ |
| storage_policy                 | storage_policy of table                 | false    | None    |
| granularity                    | index_granularity of table              | false    | 8192    |
| min_age_to_force_merge_seconds | min_age_to_force_merge_seconds of table | false    | 60      |

For more information on how Clickhouse table options work, please see the official [documentation](https://clickhouse.com/docs/operations/settings/merge-tree-settings).
