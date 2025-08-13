use crate::adapter::postgres::PostgresColumn;

pub fn generate_clickhouse_create_table_query(
    database_name: &str,
    table_name: &str,
    columns: &[PostgresColumn],
) -> String {
    let mut query = format!("CREATE TABLE {database_name}.{table_name}");
    query.push('(');

    let column_definitions: Vec<String> = columns
        .iter()
        .map(|col| {
            let clickhouse_type = col.convert_to_clickhouse_type();
            format!(
                "`{}` {} COMMENT '{}'",
                col.column_name,
                clickhouse_type.to_type_text(),
                col.comment.replace("'", "\"")
            )
        })
        .collect();

    query.push_str(&column_definitions.join(", \n"));

    let primary_keys: Vec<String> = columns
        .iter()
        .filter(|col| col.is_primary_key)
        .map(|col| col.column_name.clone())
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
