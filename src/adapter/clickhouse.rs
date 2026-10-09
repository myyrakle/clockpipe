use serde::{Deserialize, Serialize};

use crate::{adapter::IntoClickhouseValue, errors};

#[derive(Clone)]
pub struct ClickhouseConnection {
    client: clickhouse::Client,
}

#[derive(Debug, Clone, Serialize, Deserialize, clickhouse::Row)]
pub struct ClickhouseColumn {
    pub column_index: u64,
    pub column_name: String,
    pub data_type: String,
    pub is_in_primary_key: bool,
}

// https://clickhouse.com/docs/sql-reference/data-types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ClickhouseType {
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Int256,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    UInt128,
    UInt256,
    Float32,
    Float64,
    Bool,
    String,
    FixedString(u64),
    Decimal,
    Date,
    Date32,
    Time,
    Time64(u8),
    DateTime(DateTime),
    DateTime64(DateTime64),
    UUID,
    Array(Box<ClickhouseType>),
    Nullable(Box<ClickhouseType>),
    Unknown,
}

impl ClickhouseType {
    pub fn nullable(self) -> Self {
        ClickhouseType::Nullable(Box::new(self))
    }

    pub fn array(self) -> Self {
        ClickhouseType::Array(Box::new(self))
    }

    pub fn to_type_text(&self) -> String {
        match self {
            ClickhouseType::Int8 => "Int8".to_string(),
            ClickhouseType::Int16 => "Int16".to_string(),
            ClickhouseType::Int32 => "Int32".to_string(),
            ClickhouseType::Int64 => "Int64".to_string(),
            ClickhouseType::Int128 => "Int128".to_string(),
            ClickhouseType::Int256 => "Int256".to_string(),
            ClickhouseType::UInt8 => "UInt8".to_string(),
            ClickhouseType::UInt16 => "UInt16".to_string(),
            ClickhouseType::UInt32 => "UInt32".to_string(),
            ClickhouseType::UInt64 => "UInt64".to_string(),
            ClickhouseType::UInt128 => "UInt128".to_string(),
            ClickhouseType::UInt256 => "UInt256".to_string(),
            ClickhouseType::Float32 => "Float32".to_string(),
            ClickhouseType::Float64 => "Float64".to_string(),
            ClickhouseType::Bool => "Bool".to_string(),
            ClickhouseType::String => "String".to_string(),
            ClickhouseType::FixedString(size) => format!("FixedString({size})"),
            ClickhouseType::Decimal => "Decimal".to_string(),
            ClickhouseType::Date => "Date".to_string(),
            ClickhouseType::Date32 => "Date32".to_string(),
            ClickhouseType::Time => "Time".to_string(),
            ClickhouseType::Time64(precision) => format!("Time64({precision})"),
            ClickhouseType::DateTime(datetime) => datetime.to_type_text(),
            ClickhouseType::DateTime64(datetime64) => datetime64.to_type_text(),
            ClickhouseType::UUID => "UUID".to_string(),
            ClickhouseType::Array(inner_type) => format!("Array({})", inner_type.to_type_text()),
            ClickhouseType::Nullable(inner_type) => {
                format!("Nullable({})", inner_type.to_type_text())
            }
            ClickhouseType::Unknown => "Unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DateTime {
    pub timezone: Option<String>,
}

impl DateTime {
    pub fn to_type_text(&self) -> String {
        match &self.timezone {
            Some(tz) => format!("DateTime('{tz}')"),
            None => "DateTime".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateTime64 {
    pub precision: u8,
    pub timezone: Option<String>,
}

impl DateTime64 {
    pub fn to_type_text(&self) -> String {
        match &self.timezone {
            Some(tz) => format!("DateTime64({}, '{}')", self.precision, tz),
            None => format!("DateTime64({})", self.precision),
        }
    }
}

impl ClickhouseColumn {
    pub fn to_clickhouse_value(&self, value: impl IntoClickhouseValue) -> String {
        if value.is_null() & self.data_type.starts_with("Nullable") {
            return "NULL".to_string();
        }

        match self.data_type.as_str() {
            "Int8" | "Int16" | "Int32" | "Int64" | "Nullable(Int8)" | "Nullable(Int16)"
            | "Nullable(Int32)" | "Nullable(Int64)" => value.to_integer(),
            "Float32" | "Float64" | "Nullable(Float32)" | "Nullable(Float64)" => value.to_real(),
            "Bool" | "Nullable(Bool)" => value.to_bool(),
            "String" | "Nullable(String)" => value.to_string(),
            "Date" | "Date32" | "Nullable(Date)" | "Nullable(Date32)" => value.to_date(),
            "DateTime" | "DateTime64" | "Nullable(DateTime)" | "Nullable(DateTime64)" => {
                value.to_datetime()
            }
            "Time" | "Time64" | "Nullable(Time)" | "Nullable(Time64)" => value.to_time(),
            "Array(String)" => value.to_string_array(),
            "Decimal" | "Nullable(Decimal)" => value.to_real(),
            _ => {
                if self.data_type.starts_with("Array") {
                    value.to_array()
                } else if self.data_type.contains("DateTime") {
                    value.to_datetime()
                } else if self.data_type.contains("Time") {
                    value.to_time()
                } else if self.data_type.contains("String") {
                    value.to_string()
                } else {
                    value.unknown_value()
                }
            }
        }
    }
}

fn unescape_tsv_cell(cell: &str) -> Option<String> {
    if cell == "\\N" {
        return None;
    }
    let mut out = String::with_capacity(cell.len());
    let mut chars = cell.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('0') => out.push('\0'),
            Some('b') => out.push('\u{0008}'),
            Some('f') => out.push('\u{000C}'),
            Some('a') => out.push('\u{0007}'),
            Some('v') => out.push('\u{000B}'),
            Some('\\') => out.push('\\'),
            Some('\'') => out.push('\''),
            Some(other) => {
                out.push('\\');
                out.push(other);
            }
            None => out.push('\\'),
        }
    }
    Some(out)
}

impl ClickhouseConnection {
    pub fn new(config: &crate::config::ClickHouseConnectionConfig) -> Self {
        let scheme = if config.secure { "https" } else { "http" };
        let client = clickhouse::Client::default()
            .with_url(format!("{}://{}:{}", scheme, config.host, config.port))
            .with_user(config.username.as_str())
            .with_password(config.password.as_str())
            .with_database(config.database.as_str());

        log::info!(
            "Created ClickHouse connection to {}://{}:{}",
            scheme,
            config.host,
            config.port
        );

        ClickhouseConnection { client }
    }

    pub async fn ping(&self) -> errors::Result<()> {
        self.client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map_err(|e| {
                crate::errors::Errors::DatabasePingError(format!("Failed to ping ClickHouse: {e}"))
            })?;

        Ok(())
    }

    pub async fn list_columns_by_tablename(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> errors::Result<Vec<ClickhouseColumn>> {
        let result: Vec<ClickhouseColumn> = self
            .client
            .query(
                r#"
                SELECT 
                    position as column_index,
                    name as column_name,
                    type as data_type,
                    is_in_primary_key as is_primary_key
                FROM system.columns 
                WHERE table = ? AND database = ?
                ORDER BY position
            "#,
            )
            .bind(table_name)
            .bind(database_name)
            .fetch_all()
            .await
            .map_err(|e| {
                crate::errors::Errors::ListTableColumnsFailed(format!(
                    "Failed to list columns for table {table_name}: {e}"
                ))
            })?;

        Ok(result)
    }

    pub async fn execute_query(&self, query: &str) -> errors::Result<()> {
        let query = query.replace("?", "??");

        self.client.query(&query).execute().await.map_err(|e| {
            crate::errors::Errors::DatabaseQueryError(format!(
                "Failed to execute query: {e}, query: {query}"
            ))
        })?;

        Ok(())
    }

    pub async fn table_is_not_empty(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> errors::Result<bool> {
        let query = format!("select exists(select 1 from {schema_name}.{table_name}) as exists");

        let exists: bool = self
            .client
            .query(query.as_str())
            .fetch_one()
            .await
            .map_err(|e| {
                crate::errors::Errors::TableNotFoundError(format!(
                    "Failed to check if table exists: {e}"
                ))
            })?;

        Ok(exists)
    }

    /// Runs an arbitrary SELECT and returns rows as `Vec<Vec<Option<String>>>`.
    /// The query must emit `TabSeparated`-compatible columns (typically wrapped
    /// with `toString(...)`); `\N` is decoded as NULL, TSV escapes are unescaped.
    pub async fn fetch_string_rows(
        &self,
        query: &str,
    ) -> errors::Result<Vec<Vec<Option<String>>>> {
        let mut cursor = self
            .client
            .query(query)
            .fetch_bytes("TabSeparated")
            .map_err(|e| {
                crate::errors::Errors::DatabaseQueryError(format!(
                    "Failed to start fetch_bytes: {e}, query: {query}"
                ))
            })?;

        let mut buffer: Vec<u8> = Vec::new();
        while let Some(chunk) = cursor.next().await.map_err(|e| {
            crate::errors::Errors::DatabaseQueryError(format!(
                "Failed to read chunk: {e}, query: {query}"
            ))
        })? {
            buffer.extend_from_slice(&chunk);
        }

        let text = String::from_utf8(buffer).map_err(|e| {
            crate::errors::Errors::DatabaseQueryError(format!(
                "TSV response is not valid UTF-8: {e}, query: {query}"
            ))
        })?;

        let mut rows = Vec::new();
        for line in text.split('\n') {
            if line.is_empty() {
                continue;
            }
            let cells: Vec<Option<String>> = line.split('\t').map(unescape_tsv_cell).collect();
            rows.push(cells);
        }

        Ok(rows)
    }

    pub async fn truncate_table(&self, schema_name: &str, table_name: &str) -> errors::Result<()> {
        let query = format!("TRUNCATE TABLE {schema_name}.{table_name}");

        self.execute_query(&query).await.map_err(|e| {
            crate::errors::Errors::DatabaseQueryError(format!(
                "Failed to truncate table {schema_name}.{table_name}: {e}"
            ))
        })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::unescape_tsv_cell;

    #[test]
    fn unescape_tsv_cell_decodes_null_sentinel() {
        assert_eq!(unescape_tsv_cell("\\N"), None);
    }

    #[test]
    fn unescape_tsv_cell_passes_plain_text() {
        assert_eq!(unescape_tsv_cell("hello"), Some("hello".to_string()));
        assert_eq!(unescape_tsv_cell(""), Some(String::new()));
    }

    #[test]
    fn unescape_tsv_cell_decodes_standard_escapes() {
        assert_eq!(
            unescape_tsv_cell("line1\\nline2"),
            Some("line1\nline2".to_string())
        );
        assert_eq!(unescape_tsv_cell("col\\tval"), Some("col\tval".to_string()));
        assert_eq!(unescape_tsv_cell("a\\\\b"), Some("a\\b".to_string()));
        assert_eq!(unescape_tsv_cell("a\\'b"), Some("a'b".to_string()));
    }

    #[test]
    fn unescape_tsv_cell_decodes_control_escapes() {
        assert_eq!(unescape_tsv_cell("x\\by"), Some("x\u{0008}y".to_string()));
        assert_eq!(unescape_tsv_cell("x\\fy"), Some("x\u{000C}y".to_string()));
        assert_eq!(unescape_tsv_cell("x\\ay"), Some("x\u{0007}y".to_string()));
        assert_eq!(unescape_tsv_cell("x\\vy"), Some("x\u{000B}y".to_string()));
    }

    #[test]
    fn unescape_tsv_cell_preserves_unknown_backslash_pairs() {
        assert_eq!(unescape_tsv_cell("x\\zy"), Some("x\\zy".to_string()));
    }

    #[test]
    fn unescape_tsv_cell_handles_trailing_backslash() {
        assert_eq!(unescape_tsv_cell("x\\"), Some("x\\".to_string()));
    }
}
