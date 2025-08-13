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

impl ClickhouseColumn {
    pub fn default_value(&self) -> String {
        match self.data_type.as_str() {
            "Int8" | "Int16" | "Int32" | "Int64" => "0".to_string(),
            "Float32" | "Float64" => "0.0".to_string(),
            "String" => "''".to_string(),
            "Decimal" => "0.0".to_string(),
            "Date" => "current_date()".to_string(),
            "DateTime" => "now()".to_string(),
            _ => {
                if self.data_type.starts_with("Array") {
                    "[]".to_string()
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
            "String" | "Nullable(String)" => {
                format!("'{}'", value.text_or("''".to_string()).replace("'", "''"))
            }
            "Date" | "Nullable(Date)" => format!(
                "toDate('{}')",
                Self::cut_millisecond(&value.text_or("current_date()".to_string()))
            ),
            "DateTime" | "Nullable(DateTime)" => format!(
                "toDateTime('{}')",
                Self::cut_millisecond(&value.text_or("now()".to_string()))
            ),
            "Decimal" | "Nullable(Decimal)" => value.text_or("0.0".to_string()),
            _ => {
                if self.data_type.starts_with("Array") {
                    format!("[{}]", value.array_value().unwrap_or_default(),)
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
        self.client.query(query).execute().await.map_err(|e| {
            crate::errors::Errors::DatabaseConnectionError(format!("Failed to execute query: {e}"))
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
