use sqlx::postgres::PgConnectOptions;
pub mod mapper;
pub mod pgoutput;

use crate::{config::PostgresConnectionConfig, errors};

#[derive(Debug, Clone)]
pub struct PostgresConnection {
    pool: sqlx::Pool<sqlx::Postgres>,
    config: PostgresConnectionConfig,
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
                log::info!("Successfully connected to Postgres database");

                Ok(PostgresConnection {
                    pool,
                    config: config.clone(),
                })
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

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct Publication {
    pub name: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PublicationTable {
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PostgresColumnType {
    pub column_index: i32,
    pub column_name: String,
    pub data_type: String,
    pub length: i32,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub comment: String,
}

#[derive(Debug, Clone, Default)]
pub struct PostgresCopyRow {
    pub columns: Vec<Option<String>>,
}

impl PostgresConnection {
    pub async fn find_publication_by_name(
        &self,
        publication_name: &str,
    ) -> errors::Result<Option<Publication>> {
        let result: Vec<Publication> =
            sqlx::query_as("SELECT pubname as name FROM pg_publication WHERE pubname = $1")
                .bind(publication_name)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    errors::Errors::PublicationFindFailed(format!(
                        "Failed to find publication by name: {}",
                        e
                    ))
                })?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(result[0].clone()))
        }
    }

    pub async fn get_publication_tables(
        &self,
        publication_name: &str,
    ) -> errors::Result<Vec<PublicationTable>> {
        let result: Vec<PublicationTable> = sqlx::query_as(
            "SELECT schemaname as schema_name, tablename as table_name FROM pg_publication_tables WHERE pubname = $1",
        )
        .bind(publication_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            errors::Errors::PublicationFindFailed(format!(
                "Failed to get publication tables: {}",
                e
            ))
        })?;

        Ok(result)
    }

    pub async fn create_publication(
        &self,
        publication_name: &str,
        table_names: &[String],
    ) -> errors::Result<()> {
        log::debug!(
            "Creating publication {} for tables: {:?}",
            publication_name,
            table_names
        );

        let query = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            publication_name,
            table_names.join(", ")
        );

        sqlx::query(&query).execute(&self.pool).await.map_err(|e| {
            errors::Errors::PublicationCreateFailed(format!("Failed to create publication: {}", e))
        })?;

        log::info!("Successfully created publication {}", publication_name);

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
        log::debug!("Creating replication slot: {}", slot_name);

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

        log::info!("Successfully created replication slot {}", slot_name);

        Ok(())
    }

    pub async fn find_replication_slot_by_name(
        &self,
        slot_name: &str,
    ) -> errors::Result<Option<ReplicationSlot>> {
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
            return Ok(None);
        }

        Ok(Some(rows[0].clone()))
    }

    pub async fn list_columns_by_tablename(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> errors::Result<Vec<PostgresColumnType>> {
        let query = format!(
            r#"
             SELECT 
                c.ordinal_position as column_index,
                c.column_name as column_name,
                c.udt_name as data_type, 
                coalesce(c.character_maximum_length, 0) as length,
                c.is_nullable = 'YES' as nullable,
                EXISTS(
                    SELECT 1
                    FROM 
                        information_schema.table_constraints tc
                    JOIN 
                        information_schema.key_column_usage kcu 
                        ON tc.constraint_name = kcu.constraint_name
                        AND tc.table_schema = kcu.table_schema
                    WHERE 1=1
                        AND tc.constraint_type = 'PRIMARY KEY'
                        AND tc.table_schema = c.table_schema
                        AND tc.table_name = c.table_name
                        AND kcu.column_name = c.column_name
                ) as is_primary_key,
                coalesce(pgd.description, '') as comment
            FROM 
                information_schema.columns c
            LEFT JOIN 
                pg_catalog.pg_description pgd 
            ON pgd.objsubid = c.ordinal_position
            AND 
                pgd.objoid = (
                    SELECT oid 
                    FROM pg_catalog.pg_class 
                    WHERE relname = c.table_name
                )
            WHERE c.table_name = $1 AND c.table_schema = $2
        "#
        );

        let rows: Vec<PostgresColumnType> = sqlx::query_as(&query)
            .bind(table_name)
            .bind(database_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                errors::Errors::ListTableColumnsFailed(format!("Failed to get columns: {}", e))
            })?;

        Ok(rows)
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
        .bind(replication_slot_name)
        .bind(limit)
        .bind(publication_name)
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

    /// COPY TO STDOUT을 사용하여 테이블 데이터를 바이트로 다운로드
    pub async fn copy_table_to_stdout(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> errors::Result<Vec<PostgresCopyRow>> {
        let query = format!("COPY (SELECT * FROM {schema_name}.{table_name}) TO STDOUT");

        log::debug!("Executing COPY TO STDOUT query: {}", query);

        let connection_string = self.config.connection_string();

        // tokio-postgres를 사용하여 COPY TO STDOUT 실행
        let (client, connection) =
            tokio_postgres::connect(connection_string.as_str(), tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    errors::Errors::CopyTableFailed(format!(
                        "Failed to connect to PostgreSQL for COPY: {}",
                        e
                    ))
                })?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("Connection error: {}", e);
            }
        });

        // COPY TO STDOUT 실행
        let copy_sink = client.copy_out(&query).await.map_err(|e| {
            errors::Errors::CopyTableFailed(format!(
                "Failed to start COPY TO STDOUT for table {}: {}",
                table_name, e
            ))
        })?;

        // 스트림에서 모든 데이터 수집
        use futures::StreamExt;
        let mut result_data = Vec::new();

        let mut stream = Box::pin(copy_sink);
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => result_data.extend_from_slice(&bytes),
                Err(e) => {
                    return Err(errors::Errors::CopyTableFailed(format!(
                        "Error reading COPY data: {}",
                        e
                    )));
                }
            }
        }

        log::info!(
            "Successfully executed COPY TO STDOUT for table {} ({} bytes)",
            table_name,
            result_data.len()
        );

        let text = String::from_utf8(result_data.clone()).map_err(|e| {
            errors::Errors::CopyTableFailed(format!("Failed to convert bytes to string: {}", e))
        })?;

        let mut rows = Vec::new();

        let mut current_row = PostgresCopyRow {
            columns: Vec::new(),
        };
        let mut current_word = String::new();
        for c in text.chars() {
            // column separator
            if c == '\t' {
                if current_word == "\\N" {
                    current_row.columns.push(None);
                    current_word.clear();
                } else {
                    current_row
                        .columns
                        .push(Some(std::mem::take(&mut current_word)));
                }
                continue;
            }

            // row separator
            if c == '\n' {
                if !current_word.is_empty() {
                    current_row.columns.push(Some(current_word));
                    current_word = String::new();
                }

                rows.push(std::mem::take(&mut current_row));
                continue;
            }

            current_word.push(c);
        }

        Ok(rows)
    }
}
