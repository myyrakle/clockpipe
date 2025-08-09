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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct ReplicationSlot {
    pub slot_name: String,
    pub wal_status: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PeekWalChangeResult {
    pub lsn: String,
    pub xid: String,
    pub data: Vec<u8>,
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

impl PostgresConnection {
    pub async fn create_publication(
        &self,
        publication_name: &str,
        table_names: &[String],
    ) -> errors::Result<()> {
        let query = format!(
            "CREATE PUBLICATION IF NOT EXISTS {} FOR ONLY TABLES {}",
            publication_name,
            table_names.join(", ")
        );

        sqlx::query(&query).execute(&self.pool).await.map_err(|e| {
            errors::Errors::PublicationCreateFailed(format!("Failed to create publication: {}", e))
        })?;

        Ok(())
    }

    pub async fn add_table_to_publication(
        &self,
        publication_name: &str,
        table_names: &[String],
    ) -> errors::Result<()> {
        let query = format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            publication_name,
            table_names.join(", ")
        );

        sqlx::query(&query).execute(&self.pool).await.map_err(|e| {
            errors::Errors::PublicationAddFailed(format!(
                "Failed to add table to publication: {}",
                e
            ))
        })?;

        Ok(())
    }

    pub async fn create_replication_slot(&self, slot_name: &str) -> errors::Result<()> {
        sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput');")
            .bind(slot_name)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                errors::Errors::ReplicationCreateFailed(format!(
                    "Failed to create replication slot: {}",
                    e
                ))
            })?;

        Ok(())
    }

    pub async fn get_replication_slot(&self, slot_name: &str) -> errors::Result<ReplicationSlot> {
        let rows: Vec<ReplicationSlot> = sqlx::query_as(
            "select slot_name, wal_status from pg_replication_slots where slot_name = $1;",
        )
        .bind(slot_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            errors::Errors::ReplicationCreateFailed(format!(
                "Failed to create replication slot: {}",
                e
            ))
        })?;

        if rows.is_empty() {
            return Err(errors::Errors::ReplicationNotFound(format!(
                "No replication slot found with name: {}",
                slot_name
            )));
        }

        Ok(rows[0].clone())
    }

    pub async fn peek_wal_changes(
        &self,
        publication_name: &str,
        replication_slot_name: &str,
        limit: i64, // recommendation: 65536
    ) -> errors::Result<Vec<PeekWalChangeResult>> {
        let rows: Vec<PeekWalChangeResult> = sqlx::query_as(
            r#"
                SELECT lsn, xid, data 
		        FROM pg_logical_slot_peek_binary_changes($1, NULL, $2, 'proto_version', '1', 'publication_names', $3)
            "#,
        )
        .bind(publication_name)
        .bind(replication_slot_name)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            errors::Errors::PeekWalChangesFailed(format!("Failed to peek WAL changes: {}", e))
        })?;

        Ok(rows)
    }

    pub async fn advance_replication_slot(
        &self,
        replication_slot_name: &str,
        lsn: &str,
    ) -> errors::Result<()> {
        sqlx::query("SELECT pg_logical_slot_advance($1, $2);")
            .bind(replication_slot_name)
            .bind(lsn)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                errors::Errors::ReplicationSlotAdvanceFailed(format!(
                    "Failed to advance replication slot: {}",
                    e
                ))
            })?;

        Ok(())
    }
}
