use crate::{
    adapter::{clickhouse::ClickhouseType, postgres::PostgresCopyRow},
    config::ClickHouseConfig,
    pipes::PostgresPipeTableInfo,
};

pub trait IntoClickhouseColumn {
    fn into_clickhouse_type(&self) -> ClickhouseType;
    fn get_column_name(&self) -> &str;
    fn get_comment(&self) -> &str;
    fn is_in_primary_key(&self) -> bool;
}

pub trait IntoClickhouseRow {}

pub trait IntoClickhouse {
    fn generate_create_table_query(
        &self,
        database_name: &str,
        table_name: &str,
        columns: &[impl IntoClickhouseColumn],
    ) -> String {
        let mut query = format!("CREATE TABLE {database_name}.{table_name}");
        query.push('(');

        let column_definitions: Vec<String> = columns
            .iter()
            .map(|col| {
                let clickhouse_type = col.into_clickhouse_type();
                format!(
                    "`{}` {} COMMENT '{}'",
                    col.get_column_name(),
                    clickhouse_type.to_type_text(),
                    col.get_comment().replace("'", "\"")
                )
            })
            .collect();

        query.push_str(&column_definitions.join(", \n"));

        let primary_keys: Vec<String> = columns
            .iter()
            .filter(|col| col.is_in_primary_key())
            .map(|col| col.get_column_name().to_owned())
            .collect();
        let primary_keys = primary_keys.join(", ");

        query.push(')');
        query.push_str(" ENGINE = ReplacingMergeTree()");
        if !primary_keys.is_empty() {
            query.push_str(format!(" ORDER BY ({primary_keys})").as_str());
        }
        query.push_str("SETTINGS");
        query.push_str(" index_granularity = 8192,");
        query.push_str(" min_age_to_force_merge_seconds = 1");

        query.push(';');

        query
    }

    fn generate_insert_query(
        &self,
        clickhouse_config: &ClickHouseConfig,
        source_table_info: &PostgresPipeTableInfo,
        table_name: &str,
        rows: &[PostgresCopyRow],
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

        for clickhouse_column in &source_table_info.clickhouse_columns {
            let Some(postgres_column) = source_table_info
                .postgres_columns
                .iter()
                .find(|col| col.column_name == clickhouse_column.column_name)
            else {
                continue;
            };

            columns.push((clickhouse_column.clone(), postgres_column.clone()));
            column_names.push(clickhouse_column.column_name.clone());
        }

        insert_query.push_str(&format!("({}) ", column_names.join(", ")));
        insert_query.push_str("VALUES");

        let mut values = vec![];

        for row in rows {
            let mut value = vec![];

            for (clickhouse_column, _) in columns.iter() {
                let Some(postgres_column) = source_table_info
                    .postgres_columns
                    .iter()
                    .find(|col| col.column_name == clickhouse_column.column_name)
                else {
                    log::warn!(
                        "Postgres column {} not found in ClickHouse table {}. Skipping.",
                        clickhouse_column.column_name,
                        table_name
                    );
                    continue;
                };

                let postgres_raw_column_value =
                    row.columns.get(postgres_column.column_index as usize - 1);

                let column_value = match postgres_raw_column_value {
                    Some(raw_value) => clickhouse_column.value(raw_value.to_owned()),
                    _ => clickhouse_column.default_value(),
                };

                value.push(column_value);
            }

            let value = value.join(",");
            values.push(format!("({value})"));
        }

        insert_query.push_str(values.join(", ").as_str());

        insert_query
    }
}
