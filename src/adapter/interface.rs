use crate::{
    adapter::{
        clickhouse::{ClickhouseColumn, ClickhouseType},
        postgres::pgoutput::PgOutputValue,
    },
    config::ClickHouseConfig,
};

pub trait IntoClickhouseColumn {
    fn to_clickhouse_type(&self) -> ClickhouseType;
    fn get_column_name(&self) -> &str;
    fn get_column_index(&self) -> usize;
    fn get_comment(&self) -> &str;
    fn is_in_primary_key(&self) -> bool;
}

pub trait IntoClickhouseRow {
    fn find_value_by_column_name(
        &self,
        source_columns: &[impl IntoClickhouseColumn],
        column_name: &str,
    ) -> Option<impl IntoClickhouseValue>;
}

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
}

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
        clickhouse_columns: &[ClickhouseColumn],
        source_columns: &[impl IntoClickhouseColumn],
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
            let Some(postgres_column) = source_columns
                .iter()
                .find(|col| col.get_column_name() == clickhouse_column.column_name)
            else {
                continue;
            };

            columns.push((clickhouse_column.clone(), postgres_column));
            column_names.push(clickhouse_column.column_name.clone());
        }

        insert_query.push_str(&format!("({}) ", column_names.join(", ")));
        insert_query.push_str("VALUES");

        let mut values = vec![];

        for row in rows {
            let mut value = vec![];

            for (clickhouse_column, _) in columns.iter() {
                let raw_value =
                    row.find_value_by_column_name(source_columns, &clickhouse_column.column_name);

                let column_value = match raw_value {
                    Some(value) => clickhouse_column.to_clickhouse_value(value),
                    None => clickhouse_column.to_clickhouse_value(PgOutputValue::Null),
                };

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
        row: &IntoClickhouseRowType,
    ) -> String
    where
        IntoClickhouseColumnType: IntoClickhouseColumn,
        IntoClickhouseRowType: IntoClickhouseRow,
    {
        let mut delete_query = format!(
            "ALTER TABLE {}.{table_name} DELETE WHERE ",
            clickhouse_config.connection.database
        );

        let mut conditions = vec![];

        for clickhouse_column in clickhouse_columns.iter() {
            if !clickhouse_column.is_in_primary_key {
                continue;
            }

            let raw_value: Option<_> =
                row.find_value_by_column_name(source_columns, &clickhouse_column.column_name);

            let column_value = match raw_value {
                Some(value) => clickhouse_column.to_clickhouse_value(value),
                None => clickhouse_column.to_clickhouse_value(PgOutputValue::Null),
            };

            conditions.push(format!(
                "{} = {}",
                clickhouse_column.column_name, column_value
            ));
        }

        delete_query.push_str(&conditions.join(" AND "));

        delete_query
    }
}
