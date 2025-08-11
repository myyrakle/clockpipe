use serde::{Deserialize, Serialize};

use crate::errors;

#[derive(Clone)]
pub struct ClickhouseConnection {
    client: clickhouse::Client,
}

#[derive(Debug, Serialize, Deserialize, clickhouse::Row)]
pub struct ClickhouseColumn {
    pub column_index: u64,
    pub column_name: String,
    pub data_type: String,
    pub is_in_primary_key: bool,
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
                crate::errors::Errors::DatabasePingError(format!(
                    "Failed to ping ClickHouse: {}",
                    e
                ))
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
                    "Failed to list columns for table {}: {}",
                    table_name, e
                ))
            })?;

        Ok(result)
    }

    pub async fn execute_query(&self, query: &str) -> errors::Result<()> {
        self.client.query(query).execute().await.map_err(|e| {
            crate::errors::Errors::DatabaseConnectionError(format!(
                "Failed to execute query: {}",
                e
            ))
        })?;

        Ok(())
    }
}
