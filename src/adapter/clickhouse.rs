use serde::{Deserialize, Serialize};

use crate::{adapter::postgres::pgoutput::PgOutputValue, errors};

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
}

impl ClickhouseType {
    pub fn nullable(self) -> Self {
        ClickhouseType::Nullable(Box::new(self.clone()))
    }

    pub fn array(self) -> Self {
        ClickhouseType::Array(Box::new(self.clone()))
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
            ClickhouseType::FixedString(size) => format!("FixedString({})", size),
            ClickhouseType::Decimal => "Decimal".to_string(),
            ClickhouseType::Date => "Date".to_string(),
            ClickhouseType::Date32 => "Date32".to_string(),
            ClickhouseType::Time => "Time".to_string(),
            ClickhouseType::Time64(precision) => format!("Time64({})", precision),
            ClickhouseType::DateTime(datetime) => datetime.to_type_text(),
            ClickhouseType::DateTime64(datetime64) => datetime64.to_type_text(),
            ClickhouseType::UUID => "UUID".to_string(),
            ClickhouseType::Array(inner_type) => format!("Array({})", inner_type.to_type_text()),
            ClickhouseType::Nullable(inner_type) => {
                format!("Nullable({})", inner_type.to_type_text())
            }
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
            Some(tz) => format!("DateTime('{}')", tz),
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
    pub fn default_value(&self) -> String {
        match self.data_type.as_str() {
            "Int8" | "Int16" | "Int32" | "Int64" => "0".to_string(),
            "Float32" | "Float64" => "0.0".to_string(),
            "Bool" => "false".to_string(),
            "String" => "''".to_string(),
            "Decimal" => "0.0".to_string(),
            "Date" | "Date32" => "current_date()".to_string(),
            "DateTime" | "DateTime64" => "now()".to_string(),
            "Time" | "Time64" => "now()".to_string(),
            _ => {
                if self.data_type.starts_with("Array") {
                    "[]".to_string()
                } else if self.data_type.starts_with("Date") {
                    "now()".to_string()
                } else if self.data_type.starts_with("Time") {
                    "now()".to_string()
                } else {
                    "NULL".to_string() // Default for unknown types
                }
            }
        }
    }

    pub fn value(&self, value: PgOutputValue) -> String {
        if value.is_null() & self.data_type.starts_with("Nullable") {
            return "NULL".to_string();
        }

        match self.data_type.as_str() {
            "Int8" | "Int16" | "Int32" | "Int64" | "Nullable(Int8)" | "Nullable(Int16)"
            | "Nullable(Int32)" | "Nullable(Int64)" => value.text_or("0".to_string()),
            "Float32" | "Float64" | "Nullable(Float32)" | "Nullable(Float64)" => {
                value.text_or("0.0".to_string())
            }
            "Bool" | "Nullable(Bool)" => Self::parse_bool(&value.text_or("false".to_string())),
            "String" | "Nullable(String)" => {
                format!(
                    "'{}'",
                    Self::escape_single_quotes(&value.text_or("".to_string()))
                )
            }
            "Date" | "Date32" | "Nullable(Date)" | "Nullable(Date32)" => format!(
                "toDate('{}')",
                Self::cut_millisecond(&value.text_or("current_date()".to_string()))
            ),
            "DateTime" | "DateTime64" | "Nullable(DateTime)" | "Nullable(DateTime64)" => format!(
                "toDateTime('{}')",
                Self::cut_millisecond(&value.text_or("now()".to_string()))
            ),
            "Time" | "Time64" | "Nullable(Time)" | "Nullable(Time64)" => format!(
                "toTime('{}')",
                Self::cut_millisecond(&value.text_or("now()".to_string()))
            ),
            "Array(String)" => {
                let text = value.array_value().unwrap_or_default();
                let array_values = Self::parse_string_array(&text)
                    .into_iter()
                    .map(|s| format!("'{}'", Self::escape_single_quotes(&s)))
                    .collect::<Vec<String>>();

                format!("[{}]", array_values.join(", "))
            }
            "Decimal" | "Nullable(Decimal)" => value.text_or("0.0".to_string()),
            _ => {
                if self.data_type.starts_with("Array") {
                    format!("[{}]", value.array_value().unwrap_or_default(),)
                } else if self.data_type.contains("DateTime") {
                    format!(
                        "toDateTime('{}')",
                        Self::cut_millisecond(&value.text_or("now()".to_string()))
                    )
                } else if self.data_type.contains("Time") {
                    format!(
                        "toTime('{}')",
                        Self::cut_millisecond(&value.text_or("now()".to_string()))
                    )
                } else {
                    value.text_or("NULL".to_string())
                }
            }
        }
    }

    pub fn cut_millisecond(date_text: &str) -> String {
        if let Some(pos) = date_text.find('.') {
            date_text[..pos].to_string()
        } else {
            date_text.to_string()
        }
    }

    pub fn parse_bool(value: &str) -> String {
        match value.to_lowercase().as_str() {
            "t" | "1" | "true" => "TRUE".to_string(),
            "f" | "0" | "false" => "FALSE".to_string(),
            _ => "FALSE".to_string(),
        }
    }

    pub fn parse_string_array(value: &str) -> Vec<String> {
        let value = value.trim_matches(|c| c == '{' || c == '}');

        let trimmed = value.trim_matches('"');
        let items: Vec<String> = trimmed.split("\",\"").map(|s| s.to_string()).collect();
        items
    }

    pub fn escape_single_quotes(input: &str) -> String {
        input.replace('\'', "''")
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_parse_string_array() {
        struct TestCase {
            input: &'static str,
            expected: Vec<String>,
        }

        let test_cases = vec![
            TestCase {
                input: "{\"Flower design\",\"Pearl embellishments\",\"Stud earrings\",\"Gold accents\",\"Pearl accents\",\"Diamond accents\"}",
                expected: vec![
                    "Flower design".to_string(),
                    "Pearl embellishments".to_string(),
                    "Stud earrings".to_string(),
                    "Gold accents".to_string(),
                    "Pearl accents".to_string(),
                    "Diamond accents".to_string(),
                ],
            },
            TestCase {
                input: "{\"Button closure\",\"White stripes on collar, cuffs, and hem\"}",
                expected: vec![
                    "Button closure".to_string(),
                    "White stripes on collar, cuffs, and hem".to_string(),
                ],
            },
        ];

        for test_case in test_cases {
            let result = super::ClickhouseColumn::parse_string_array(test_case.input);
            assert_eq!(
                result, test_case.expected,
                "Failed for input: {}",
                test_case.input
            );
        }
    }
}

impl ClickhouseConnection {
    pub fn new(config: &crate::config::ClickHouseConnectionConfig) -> Self {
        let client = clickhouse::Client::default()
            .with_url(format!("http://{}:{}", config.host, config.port))
            .with_user(config.username.clone())
            .with_password(config.password.clone())
            .with_database(config.database.clone());

        log::info!(
            "Created ClickHouse connection to {}:{}",
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
}
