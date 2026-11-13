pub mod clickhouse;
pub mod mongodb;
pub mod postgres;

use std::collections::HashSet;

use crate::{
    adapter::clickhouse::{ClickhouseColumn, ClickhouseType},
    config::{
        ClickHouseConfig, ClickHouseTableOptions,
        default::clickhouse::{
            DELETE_BATCH_SIZE, INDEX_GRANULARITY, MIN_AGE_TO_FORCE_MERGE_SECONDS,
        },
    },
};

/// Trait for converting source types to Clickhouse column representation
pub trait IntoClickhouseColumn {
    fn to_clickhouse_type(&self) -> ClickhouseType;
    fn get_column_name(&self) -> &str;
    fn get_column_index(&self) -> usize;
    fn get_comment(&self) -> &str;
    fn is_in_primary_key(&self) -> bool;
}

/// Trait for converting source data row to Clickhouse row representation
pub trait IntoClickhouseRow {
    fn find_value_by_column_name(
        &self,
        source_columns: &[impl IntoClickhouseColumn],
        column_name: &str,
    ) -> Option<impl IntoClickhouseValue + Default>;

    fn debug_all(&self);
}

/// Trait for converting each source value to Clickhouse value representation
pub trait IntoClickhouseValue {
    fn to_integer(self) -> String;
    fn to_real(self) -> String;
    fn to_bool(self) -> String;
    fn to_string(self) -> String;
    fn to_date(self) -> String;
    fn to_datetime(self) -> String;
    fn to_time(self) -> String;
    fn to_array(self) -> String;
    fn to_string_array(self) -> String;
    fn unknown_value(self) -> String;

    fn is_null(&self) -> bool;
    fn into_null(self) -> Self;
}

/// Trait for generating Clickhouse queries
pub trait IntoClickhouse {
    fn generate_create_table_query(
        &self,
        clickhouse_config: &ClickHouseConfig,
        table_options: &ClickHouseTableOptions,
        table_name: &str,
        columns: &[impl IntoClickhouseColumn],
        comment: &str,
    ) -> String {
        let database_name = &clickhouse_config.connection.database;

        let mut query = format!("CREATE TABLE {database_name}.{table_name}");
        query.push('(');

        let column_definitions: Vec<String> = columns
            .iter()
            .map(|col| {
                let clickhouse_type = col.to_clickhouse_type();
                format!(
                    "`{}` {} COMMENT '{}'",
                    col.get_column_name(),
                    clickhouse_type.to_type_text(),
                    col.get_comment().replace("'", "\"")
                )
            })
            .collect();

        query.push_str(&column_definitions.join(", \n"));

        let primary_keys = columns
            .iter()
            .filter(|col| col.is_in_primary_key())
            .map(|col| col.get_column_name())
            .collect::<Vec<_>>()
            .join(", ");

        query.push(')');
        query.push_str(" ENGINE = ReplacingMergeTree()\n");
        if !primary_keys.is_empty() {
            query.push_str(format!("ORDER BY ({primary_keys})\n").as_str());
        }

        query.push_str("SETTINGS\n");

        let granularity = table_options.granularity.unwrap_or(INDEX_GRANULARITY);
        query.push_str(format!("index_granularity = {granularity}\n",).as_str());

        let min_age_to_force_merge_seconds = table_options
            .min_age_to_force_merge_seconds
            .unwrap_or(MIN_AGE_TO_FORCE_MERGE_SECONDS);
        query.push_str(
            format!(", min_age_to_force_merge_seconds = {min_age_to_force_merge_seconds}\n",)
                .as_str(),
        );

        if let Some(storage_policy) = &table_options.storage_policy {
            query.push_str(
                format!(
                    ", storage_policy = '{}'\n",
                    storage_policy.replace("'", "''")
                )
                .as_str(),
            );
        }

        query.push_str(format!("COMMENT '{}'\n", comment.replace("'", "''")).as_str());

        query.push(';');

        query
    }

    fn generate_add_column_query(
        &self,
        clickhouse_config: &ClickHouseConfig,
        table_name: &str,
        source_column: &impl IntoClickhouseColumn,
    ) -> String {
        let database_name = &clickhouse_config.connection.database;
        let column_name = source_column.get_column_name();
        let column_type = source_column.to_clickhouse_type().to_type_text();
        let column_comment = source_column.get_comment().replace("'", "\"");

        let add_column_query = format!(
            "ALTER TABLE {database_name}.{table_name} ADD COLUMN `{column_name}` {column_type} COMMENT '{column_comment}';"
        );

        add_column_query
    }

    fn generate_insert_query(
        &self,
        clickhouse_config: &ClickHouseConfig,
        clickhouse_columns: &[ClickhouseColumn],
        source_columns: &[impl IntoClickhouseColumn],
        mask_columns: &[String],
        table_name: &str,
        rows: &[impl IntoClickhouseRow],
    ) -> String {
        if rows.is_empty() {
            return String::new();
        }

        let mut insert_query = format!(
            "INSERT INTO {}.{table_name} ",
            clickhouse_config.connection.database
        );

        let mut columns = vec![];
        let mut column_names = vec![];

        for clickhouse_column in clickhouse_columns {
            columns.push(clickhouse_column);
            column_names.push(clickhouse_column.column_name.as_str());
        }

        insert_query.push_str(&format!("({}) ", column_names.join(", ")));
        insert_query.push_str("VALUES");

        let mut values = vec![];

        for row in rows {
            let mut value = vec![];

            for clickhouse_column in columns.iter() {
                let raw_value =
                    row.find_value_by_column_name(source_columns, &clickhouse_column.column_name);

                let mut raw_value = raw_value.unwrap_or_default();

                if mask_columns.contains(&clickhouse_column.column_name) {
                    raw_value = raw_value.into_null();
                }

                let column_value = clickhouse_column.to_clickhouse_value(raw_value);

                value.push(column_value);
            }

            let value = value.join(",");
            values.push(format!("({value})"));
        }

        insert_query.push_str(values.join(", ").as_str());

        insert_query
    }

    /// Generates `ALTER TABLE ... DELETE` queries for the given rows.
    ///
    /// Rows are split into chunks of `DELETE_BATCH_SIZE`, one query per chunk.
    /// Keys are matched with `IN` instead of `(pk = v1) OR (pk = v2) OR ...`: the parser folds a
    /// constant `IN` list into a single literal, so the AST stays small. A long `OR` chain makes
    /// background mutation execution fail with `Query tree is too big` (max_expanded_ast_elements),
    /// and a failed mutation blocks every later mutation on the same parts.
    fn generate_delete_queries<IntoClickhouseColumnType, IntoClickhouseRowType>(
        &self,
        clickhouse_config: &ClickHouseConfig,
        clickhouse_columns: &[ClickhouseColumn],
        source_columns: &[IntoClickhouseColumnType],
        table_name: &str,
        rows: &[IntoClickhouseRowType],
    ) -> Vec<String>
    where
        IntoClickhouseColumnType: IntoClickhouseColumn,
        IntoClickhouseRowType: IntoClickhouseRow,
    {
        let primary_key_columns: Vec<_> = clickhouse_columns
            .iter()
            .filter(|col| col.is_in_primary_key)
            .collect();

        if rows.is_empty() || primary_key_columns.is_empty() {
            return vec![];
        }

        let key_expression = if primary_key_columns.len() == 1 {
            primary_key_columns[0].column_name.clone()
        } else {
            let column_names: Vec<_> = primary_key_columns
                .iter()
                .map(|col| col.column_name.as_str())
                .collect();
            format!("({})", column_names.join(", "))
        };

        let mut delete_queries = vec![];

        for chunk in rows.chunks(DELETE_BATCH_SIZE) {
            let mut keys = vec![];

            for row in chunk.iter() {
                let mut key_values = vec![];

                for clickhouse_column in primary_key_columns.iter() {
                    let raw_value: Option<_> = row
                        .find_value_by_column_name(source_columns, &clickhouse_column.column_name);

                    let column_value =
                        clickhouse_column.to_clickhouse_value(raw_value.unwrap_or_default());

                    key_values.push(column_value);
                }

                if key_values.len() == 1 {
                    keys.push(key_values.remove(0));
                } else {
                    keys.push(format!("({})", key_values.join(", ")));
                }
            }

            delete_queries.push(format!(
                "ALTER TABLE {}.{table_name} DELETE WHERE {key_expression} IN ({})",
                clickhouse_config.connection.database,
                keys.join(", ")
            ));
        }

        delete_queries
    }
}

/// Deduplicates rows by a key derived from each row, keeping the last occurrence per key.
/// The relative order of first-seen keys is preserved.
pub fn deduplicate_rows_keeping_last<T>(rows: Vec<T>, key_fn: impl Fn(&T) -> String) -> Vec<T> {
    let mut seen = HashSet::new();
    let mut result: Vec<T> = rows
        .into_iter()
        .rev()
        .filter(|row| seen.insert(key_fn(row)))
        .collect();
    result.reverse();
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        adapter::postgres::{PostgresColumn, PostgresCopyRow, pgoutput::PgOutputValue},
        config::{ClickHouseConfig, ClickHouseConnectionConfig},
    };

    struct TestPipe;
    impl IntoClickhouse for TestPipe {}

    fn clickhouse_config() -> ClickHouseConfig {
        ClickHouseConfig {
            connection: ClickHouseConnectionConfig {
                host: "localhost".to_string(),
                port: 8123,
                username: "default".to_string(),
                password: String::new(),
                database: "db".to_string(),
                secure: false,
            },
            disable_sync_loop: false,
            table_options: Default::default(),
        }
    }

    fn postgres_column(index: i32, name: &str, data_type: &str, is_pk: bool) -> PostgresColumn {
        PostgresColumn {
            column_index: index,
            column_name: name.to_string(),
            data_type: data_type.to_string(),
            length: -1,
            nullable: false,
            is_primary_key: is_pk,
            comment: String::new(),
        }
    }

    fn clickhouse_column(index: u64, name: &str, data_type: &str, is_pk: bool) -> ClickhouseColumn {
        ClickhouseColumn {
            column_index: index,
            column_name: name.to_string(),
            data_type: data_type.to_string(),
            is_in_primary_key: is_pk,
        }
    }

    fn row(values: &[&str]) -> PostgresCopyRow {
        PostgresCopyRow {
            columns: values
                .iter()
                .map(|v| PgOutputValue::Text(v.to_string()))
                .collect(),
        }
    }

    #[test]
    fn delete_queries_use_in_list_for_single_primary_key() {
        let source_columns = vec![
            postgres_column(1, "id", "int8", true),
            postgres_column(2, "name", "text", false),
        ];
        let clickhouse_columns = vec![
            clickhouse_column(1, "id", "Int64", true),
            clickhouse_column(2, "name", "String", false),
        ];
        let rows = vec![row(&["1", "a"]), row(&["2", "b"]), row(&["3", "c"])];

        let queries = TestPipe.generate_delete_queries(
            &clickhouse_config(),
            &clickhouse_columns,
            &source_columns,
            "users",
            &rows,
        );

        assert_eq!(
            queries,
            vec!["ALTER TABLE db.users DELETE WHERE id IN (1, 2, 3)".to_string()]
        );
    }

    #[test]
    fn delete_queries_escape_string_keys() {
        let source_columns = vec![postgres_column(1, "code", "text", true)];
        let clickhouse_columns = vec![clickhouse_column(1, "code", "String", true)];
        let rows = vec![row(&["a'b"]), row(&["c\\d"])];

        let queries = TestPipe.generate_delete_queries(
            &clickhouse_config(),
            &clickhouse_columns,
            &source_columns,
            "codes",
            &rows,
        );

        assert_eq!(
            queries,
            vec![r"ALTER TABLE db.codes DELETE WHERE code IN ('a''b', 'c\\d')".to_string()]
        );
    }

    #[test]
    fn delete_queries_use_tuple_in_for_composite_primary_key() {
        let source_columns = vec![
            postgres_column(1, "tenant_id", "int8", true),
            postgres_column(2, "id", "uuid", true),
            postgres_column(3, "value", "text", false),
        ];
        let clickhouse_columns = vec![
            clickhouse_column(1, "tenant_id", "Int64", true),
            clickhouse_column(2, "id", "String", true),
            clickhouse_column(3, "value", "String", false),
        ];
        let rows = vec![row(&["1", "u1", "x"]), row(&["2", "u2", "y"])];

        let queries = TestPipe.generate_delete_queries(
            &clickhouse_config(),
            &clickhouse_columns,
            &source_columns,
            "items",
            &rows,
        );

        assert_eq!(
            queries,
            vec![
                "ALTER TABLE db.items DELETE WHERE (tenant_id, id) IN ((1, 'u1'), (2, 'u2'))"
                    .to_string()
            ]
        );
    }

    #[test]
    fn delete_queries_are_chunked_by_batch_size() {
        let source_columns = vec![postgres_column(1, "id", "int8", true)];
        let clickhouse_columns = vec![clickhouse_column(1, "id", "Int64", true)];
        let total = DELETE_BATCH_SIZE * 2 + 5;
        let ids: Vec<String> = (0..total).map(|i| i.to_string()).collect();
        let rows: Vec<_> = ids.iter().map(|id| row(&[id.as_str()])).collect();

        let queries = TestPipe.generate_delete_queries(
            &clickhouse_config(),
            &clickhouse_columns,
            &source_columns,
            "events",
            &rows,
        );

        assert_eq!(queries.len(), 3);
        let counts: Vec<usize> = queries
            .iter()
            .map(|q| q.matches(", ").count() + 1)
            .collect();
        assert_eq!(counts, vec![DELETE_BATCH_SIZE, DELETE_BATCH_SIZE, 5]);
        assert!(queries.iter().all(|q| !q.contains(" OR ")));
        assert!(queries[2].ends_with("IN (20000, 20001, 20002, 20003, 20004)"));
    }

    #[test]
    fn delete_queries_empty_without_rows_or_primary_key() {
        let source_columns = vec![postgres_column(1, "id", "int8", false)];
        let no_pk = vec![clickhouse_column(1, "id", "Int64", false)];
        let with_pk = vec![clickhouse_column(1, "id", "Int64", true)];
        let rows = vec![row(&["1"])];
        let no_rows: Vec<PostgresCopyRow> = vec![];

        assert!(
            TestPipe
                .generate_delete_queries(&clickhouse_config(), &no_pk, &source_columns, "t", &rows)
                .is_empty()
        );
        assert!(
            TestPipe
                .generate_delete_queries(
                    &clickhouse_config(),
                    &with_pk,
                    &source_columns,
                    "t",
                    &no_rows
                )
                .is_empty()
        );
    }
}
