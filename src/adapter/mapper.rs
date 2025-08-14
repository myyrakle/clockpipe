use crate::adapter::clickhouse::ClickhouseType;

pub trait IntoClickhouseColumn {
    fn into_clickhouse_type(&self) -> ClickhouseType;
    fn get_column_name(&self) -> &str;
    fn get_comment(&self) -> &str;
    fn is_in_primary_key(&self) -> bool;
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
}
