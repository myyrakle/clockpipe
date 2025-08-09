use sqlx::postgres::PgConnectOptions;

use crate::{config::PostgresConnectionConfig, errors};

#[derive(Debug, Clone)]
pub struct PostgresConnection {
    pool: sqlx::Pool<sqlx::Postgres>,
}

impl PostgresConnection {
    pub async fn new(config: &PostgresConnectionConfig) -> errors::Result<Self> {
        let mut options = PgConnectOptions::new()
            .host(&config.host)
            .port(config.port as u16)
            .username(&config.username)
            .database(&config.database);

        if !config.password.is_empty() {
            options = options.password(&config.password);
        }

        let result = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await;

        match result {
            Ok(pool) => {
                println!("Successfully connected to Postgres database");

                Ok(PostgresConnection { pool })
            }
            Err(e) => {
                return Err(errors::Errors::DatabaseConnectionError(format!(
                    "Failed to connect to Postgres database: {}",
                    e
                )));
            }
        }
    }

    pub async fn ping(&self) -> errors::Result<()> {
        let result = sqlx::query("SELECT 1").execute(&self.pool).await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(errors::Errors::DatabasePingError(format!(
                "Failed to ping Postgres database: {}",
                e
            ))),
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PostgreColumn {
    pub name: String,
    pub data_type: String,
    pub not_null: bool,
    pub position: i32,
    pub has_default: bool,
}

impl PostgresConnection {
    pub async fn get_table_name_by_relation_id(&self, relation_id: i64) -> errors::Result<String> {
        let result: Vec<(String,)> =
            sqlx::query_as("SELECT relname FROM pg_catalog.pg_class WHERE oid = $1")
                .bind(relation_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    errors::Errors::TableNotFoundError(format!(
                        "Failed to get table name by relation ID: {}",
                        e
                    ))
                })?;

        if result.is_empty() {
            return Err(errors::Errors::TableNotFoundError(format!(
                "No table found for relation ID: {}",
                relation_id
            )));
        }

        Ok(result[0].0.clone())
    }

    pub async fn get_columns_by_relation_id(
        &self,
        relation_id: i64,
    ) -> errors::Result<Vec<PostgreColumn>> {
        let result: Vec<PostgreColumn> = sqlx::query_as(
            r#"
                SELECT 
                    a.attname AS column_name,
                    pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                    a.attnotnull AS not_null,
                    a.attnum AS position,
                    a.atthasdef AS has_default
                FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = $1
                AND a.attnum > 0
                AND NOT a.attisdropped
                ORDER BY a.attnum
            "#,
        )
        .bind(relation_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            errors::Errors::GetTableNameFailed(format!(
                "Failed to get columns for relation ID: {}",
                e
            ))
        })?;

        Ok(result)
    }
}
