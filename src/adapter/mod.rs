pub mod clickhouse;
pub mod mongodb;
pub mod postgres;

use crate::{
    adapter::clickhouse::{ClickhouseColumn, ClickhouseType},
    config::{
        ClickHouseConfig, ClickHouseTableOptions,
        default::clickhouse::{INDEX_GRANULARITY, MIN_AGE_TO_FORCE_MERGE_SECONDS},
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

        query.push_str(
            format!(", min_age_to_force_merge_seconds = {MIN_AGE_TO_FORCE_MERGE_SECONDS}\n",)
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

    fn generate_delete_query<IntoClickhouseColumnType, IntoClickhouseRowType>(
        &self,
        clickhouse_config: &ClickHouseConfig,
        clickhouse_columns: &[ClickhouseColumn],
        source_columns: &[IntoClickhouseColumnType],
        table_name: &str,
        rows: &[IntoClickhouseRowType],
    ) -> String
    where
        IntoClickhouseColumnType: IntoClickhouseColumn,
        IntoClickhouseRowType: IntoClickhouseRow,
    {
        if rows.is_empty() {
            return String::new();
        }

        let mut delete_query = format!(
            "ALTER TABLE {}.{table_name} DELETE WHERE ",
            clickhouse_config.connection.database
        );

        let primary_key_columns: Vec<_> = clickhouse_columns
            .iter()
            .filter(|col| col.is_in_primary_key)
            .collect();

        let mut conditions = vec![];

        for row in rows.iter() {
            let mut conditions_per_row = vec![];

            for clickhouse_column in primary_key_columns.iter() {
                let raw_value: Option<_> =
                    row.find_value_by_column_name(source_columns, &clickhouse_column.column_name);

                let column_value =
                    clickhouse_column.to_clickhouse_value(raw_value.unwrap_or_default());

                conditions_per_row.push(format!(
                    "{} = {}",
                    clickhouse_column.column_name, column_value
                ));
            }

            conditions.push(format!("({})", conditions_per_row.join(" AND ")));
        }

        if conditions.is_empty() {
            return String::new();
        }

        delete_query.push_str(&conditions.join(" OR "));

        delete_query
    }
}
