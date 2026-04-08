use sqlx::postgres::PgConnectOptions;
pub mod pgoutput;

use crate::{
    adapter::{
        IntoClickhouseColumn, IntoClickhouseRow, IntoClickhouseValue, clickhouse::ClickhouseType,
        postgres::pgoutput::PgOutputValue,
    },
    config::PostgresConnectionConfig,
    errors,
};

#[derive(Debug, Clone)]
pub struct PostgresConnection {
    pool: sqlx::Pool<sqlx::Postgres>,
    config: PostgresConnectionConfig,
}

impl PostgresConnection {
    pub async fn new(config: &PostgresConnectionConfig) -> errors::Result<Self> {
        let mut options = PgConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .username(&config.username)
            .database(&config.database);

        if !config.password.is_empty() {
            options = options.password(&config.password);
        }

        // Apply SSL configuration
        options = match &config.ssl_mode {
            crate::config::PostgresSslMode::Disable => {
                options.ssl_mode(sqlx::postgres::PgSslMode::Disable)
            }
            crate::config::PostgresSslMode::Prefer => {
                options.ssl_mode(sqlx::postgres::PgSslMode::Prefer)
            }
            crate::config::PostgresSslMode::Require => {
                options.ssl_mode(sqlx::postgres::PgSslMode::Require)
            }
            crate::config::PostgresSslMode::VerifyCa => {
                options.ssl_mode(sqlx::postgres::PgSslMode::VerifyCa)
            }
            crate::config::PostgresSslMode::VerifyFull => {
                options.ssl_mode(sqlx::postgres::PgSslMode::VerifyFull)
            }
        };

        // Apply SSL root certificate if provided
        if let Some(ssl_root_cert) = &config.ssl_root_cert {
            options = options.ssl_root_cert(ssl_root_cert);
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
            Err(e) => Err(errors::Errors::DatabaseConnectionError(format!(
                "Failed to connect to Postgres database: {e}"
            ))),
        }
    }

    pub async fn ping(&self) -> errors::Result<()> {
        let result = sqlx::query("SELECT 1").execute(&self.pool).await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(errors::Errors::DatabasePingError(format!(
                "Failed to ping Postgres database: {e}"
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

#[derive(Debug, Clone, sqlx::FromRow, Default)]
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
        let mut result: Vec<(String,)> =
            sqlx::query_as("SELECT relname FROM pg_catalog.pg_class WHERE oid = $1")
                .bind(relation_id)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    errors::Errors::TableNotFoundError(format!(
                        "Failed to get table name by relation ID: {e}"
                    ))
                })?;

        if result.is_empty() {
            return Err(errors::Errors::TableNotFoundError(format!(
                "No table found for relation ID: {relation_id}"
            )));
        }

        Ok(std::mem::take(&mut result[0].0))
    }

    pub async fn get_relation_id_by_table_name(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> errors::Result<u32> {
        let result: Vec<(i32,)> = sqlx::query_as(
            "SELECT c.oid::INTEGER FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname = $1 AND n.nspname = $2"
        )
        .bind(table_name)
        .bind(schema_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            errors::Errors::TableNotFoundError(format!(
                "Failed to get relation ID for table {schema_name}.{table_name}: {e}"
            ))
        })?;

        if result.is_empty() {
            return Err(errors::Errors::TableNotFoundError(format!(
                "No table found for {schema_name}.{table_name}"
            )));
        }

        Ok(result[0].0 as u32)
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
                "Failed to get columns for relation ID: {e}"
            ))
        })?;

        Ok(result)
    }
}

#[derive(Debug, Clone, sqlx::FromRow, Default)]
pub struct Publication {
    pub name: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PublicationTable {
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct PostgresColumn {
    pub column_index: i32,
    pub column_name: String,
    pub data_type: String,
    pub length: i32,
    pub nullable: bool,
    pub is_primary_key: bool,
    pub comment: String,
}

impl IntoClickhouseColumn for PostgresColumn {
    fn to_clickhouse_type(&self) -> ClickhouseType {
        match self.data_type.as_str() {
            "int2" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::Int16)
                } else {
                    ClickhouseType::Int16
                }
            }
            "_int2" => ClickhouseType::array(ClickhouseType::Int16),
            "int4" | "int" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::Int32)
                } else {
                    ClickhouseType::Int32
                }
            }
            "_int4" => ClickhouseType::array(ClickhouseType::Int32),
            "int8" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::Int64)
                } else {
                    ClickhouseType::Int64
                }
            }
            "_int8" => ClickhouseType::array(ClickhouseType::Int64),
            "float4" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::Float32)
                } else {
                    ClickhouseType::Float32
                }
            }
            "_float4" => ClickhouseType::array(ClickhouseType::Float32),
            "float8" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::Float64)
                } else {
                    ClickhouseType::Float64
                }
            }
            "_float8" => ClickhouseType::array(ClickhouseType::Float64),
            "numeric" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::Decimal)
                } else {
                    ClickhouseType::Decimal
                }
            }
            "_numeric" => ClickhouseType::array(ClickhouseType::Decimal),
            // varchar
            "varchar" | "text" | "json" | "jsonb" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::String)
                } else {
                    ClickhouseType::String
                }
            }
            "_varchar" => ClickhouseType::array(ClickhouseType::String),
            "_text" => ClickhouseType::array(ClickhouseType::String),
            // Boolean
            "bool" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::Bool)
                } else {
                    ClickhouseType::Bool
                }
            }
            "_bool" => ClickhouseType::array(ClickhouseType::Bool),
            // time
            "timestamp" | "timestamptz" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::DateTime(Default::default()))
                } else {
                    ClickhouseType::DateTime(Default::default())
                }
            }
            "date" => {
                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::Date)
                } else {
                    ClickhouseType::Date
                }
            }
            _ => {
                log::warn!(
                    "Unsupported Postgres data type: {}. Defaulting to String.",
                    &self.data_type
                );

                if self.nullable {
                    ClickhouseType::nullable(ClickhouseType::String)
                } else {
                    ClickhouseType::String
                }
            }
        }
    }

    fn get_column_name(&self) -> &str {
        &self.column_name
    }

    fn get_column_index(&self) -> usize {
        self.column_index as usize
    }

    fn get_comment(&self) -> &str {
        &self.comment
    }

    fn is_in_primary_key(&self) -> bool {
        self.is_primary_key
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostgresCopyRow {
    pub columns: Vec<PgOutputValue>,
}

impl IntoClickhouseRow for PostgresCopyRow {
    fn find_value_by_column_name(
        &self,
        source_columns: &[impl IntoClickhouseColumn],
        column_name: &str,
    ) -> Option<impl IntoClickhouseValue + Default> {
        let Some(source_column) = source_columns
            .iter()
            .find(|col| col.get_column_name() == column_name)
        else {
            return Some(PgOutputValue::Null);
        };

        let index = source_column.get_column_index() - 1; // Convert to 0-based index

        let postgres_raw_column_value = self.columns.get(index);

        postgres_raw_column_value.map(ToOwned::to_owned)
    }

    fn debug_all(&self) {
        for (index, column_value) in self.columns.iter().enumerate() {
            println!("Column index: {}, Value: {:?}", index + 1, column_value);
        }
    }
}

impl PostgresConnection {
    fn finalize_copy_field(current_word: &mut String) -> PgOutputValue {
        if current_word == "\\N" {
            current_word.clear();
            return PgOutputValue::Null;
        }

        let raw = std::mem::take(current_word);

        PgOutputValue::Text(Self::decode_copy_text_field(&raw))
    }

    fn decode_copy_text_field(input: &str) -> String {
        let mut decoded = String::with_capacity(input.len());
        let chars: Vec<char> = input.chars().collect();
        let mut index = 0;

        while index < chars.len() {
            if chars[index] != '\\' {
                decoded.push(chars[index]);
                index += 1;
                continue;
            }

            index += 1;

            if index >= chars.len() {
                decoded.push('\\');
                break;
            }

            match chars[index] {
                'b' => {
                    decoded.push('\u{0008}');
                    index += 1;
                }
                'f' => {
                    decoded.push('\u{000C}');
                    index += 1;
                }
                'n' => {
                    decoded.push('\n');
                    index += 1;
                }
                'r' => {
                    decoded.push('\r');
                    index += 1;
                }
                't' => {
                    decoded.push('\t');
                    index += 1;
                }
                'v' => {
                    decoded.push('\u{000B}');
                    index += 1;
                }
                '\\' => {
                    decoded.push('\\');
                    index += 1;
                }
                'x' => {
                    let end = usize::min(index + 3, chars.len());
                    let hex_end = chars[index + 1..end]
                        .iter()
                        .take_while(|c| c.is_ascii_hexdigit())
                        .count()
                        + index
                        + 1;

                    if hex_end > index + 1 {
                        let hex: String = chars[index + 1..hex_end].iter().collect();
                        if let Ok(value) = u8::from_str_radix(&hex, 16) {
                            decoded.push(value as char);
                            index = hex_end;
                            continue;
                        }
                    }

                    decoded.push('x');
                    index += 1;
                }
                '0'..='7' => {
                    let start = index;
                    let end = usize::min(index + 3, chars.len());
                    let octal_end = chars[start..end]
                        .iter()
                        .take_while(|c| matches!(c, '0'..='7'))
                        .count()
                        + start;

                    let octal: String = chars[start..octal_end].iter().collect();

                    if let Ok(value) = u8::from_str_radix(&octal, 8) {
                        decoded.push(value as char);
                        index = octal_end;
                    } else {
                        decoded.push(chars[index]);
                        index += 1;
                    }
                }
                other => {
                    decoded.push(other);
                    index += 1;
                }
            }
        }

        decoded
    }

    pub async fn find_publication_by_name(
        &self,
        publication_name: &str,
    ) -> errors::Result<Option<Publication>> {
        let mut result: Vec<Publication> =
            sqlx::query_as("SELECT pubname as name FROM pg_publication WHERE pubname = $1")
                .bind(publication_name)
                .fetch_all(&self.pool)
                .await
                .map_err(|e| {
                    errors::Errors::PublicationFindFailed(format!(
                        "Failed to find publication by name: {e}"
                    ))
                })?;

        if result.is_empty() {
            Ok(None)
        } else {
            Ok(Some(std::mem::take(&mut result[0])))
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
                "Failed to get publication tables: {e}"
            ))
        })?;

        Ok(result)
    }

    pub async fn create_publication(
        &self,
        publication_name: &str,
        table_names: &[String],
    ) -> errors::Result<()> {
        log::debug!("Creating publication {publication_name} for tables: {table_names:?}");

        let query = format!(
            "CREATE PUBLICATION {} FOR TABLE {}",
            publication_name,
            table_names.join(", ")
        );

        sqlx::query(&query).execute(&self.pool).await.map_err(|e| {
            errors::Errors::PublicationCreateFailed(format!("Failed to create publication: {e}"))
        })?;

        log::info!("Successfully created publication {publication_name}");

        Ok(())
    }

    pub async fn add_table_to_publication(
        &self,
        publication_name: &str,
        table_names: &[&str],
    ) -> errors::Result<()> {
        let query = format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            publication_name,
            table_names.join(", ")
        );

        sqlx::query(&query).execute(&self.pool).await.map_err(|e| {
            errors::Errors::PublicationAddFailed(format!("Failed to add table to publication: {e}"))
        })?;

        Ok(())
    }

    pub async fn create_replication_slot(&self, slot_name: &str) -> errors::Result<()> {
        log::debug!("Creating replication slot: {slot_name}");

        sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput');")
            .bind(slot_name)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                errors::Errors::ReplicationCreateFailed(format!(
                    "Failed to create replication slot: {e}"
                ))
            })?;

        log::info!("Successfully created replication slot {slot_name}");

        Ok(())
    }

    pub async fn find_replication_slot_by_name(
        &self,
        slot_name: &str,
    ) -> errors::Result<Option<ReplicationSlot>> {
        let mut rows: Vec<ReplicationSlot> = sqlx::query_as(
            "select slot_name, wal_status from pg_replication_slots where slot_name = $1;",
        )
        .bind(slot_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            errors::Errors::ReplicationCreateFailed(format!(
                "Failed to create replication slot: {e}"
            ))
        })?;

        if rows.is_empty() {
            return Ok(None);
        }

        let row = std::mem::take(&mut rows[0]);

        Ok(Some(row))
    }

    pub async fn get_comment_from_table(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> errors::Result<String> {
        let result: Vec<(String,)> = sqlx::query_as(
            r#"
            SELECT
                pd.description as comment
            FROM
                pg_catalog.pg_description pd
            JOIN
                pg_catalog.pg_class pc
            ON
                pd.objoid = pc.oid
            JOIN
                pg_catalog.pg_namespace pn
            ON
                pc.relnamespace = pn.oid
            WHERE
                pc.relname = $1
                AND pn.nspname = $2
            "#,
        )
        .bind(table_name)
        .bind(database_name)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            errors::Errors::GetTableNameFailed(format!("Failed to get table comment: {e}"))
        })?;

        if result.is_empty() {
            return Ok(String::new());
        }

        Ok(result[0].0.clone())
    }

    pub async fn list_columns_by_tablename(
        &self,
        database_name: &str,
        table_name: &str,
    ) -> errors::Result<Vec<PostgresColumn>> {
        let query = r#"
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
            ORDER BY c.ordinal_position ASC
        "#
        .to_string();

        let rows: Vec<PostgresColumn> = sqlx::query_as(&query)
            .bind(table_name)
            .bind(database_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                errors::Errors::ListTableColumnsFailed(format!("Failed to get columns: {e}"))
            })?;

        let rows = rows
            .into_iter()
            .enumerate()
            .map(|(usize, mut row)| {
                row.column_index = usize as i32 + 1; // Ensure column_index starts from 1
                row
            })
            .collect::<Vec<_>>();

        Ok(rows)
    }

    pub async fn count_table_rows(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> errors::Result<i64> {
        let query = format!(
            r#"
            SELECT c.reltuples::bigint AS estimate
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = '{table_name}'
            AND n.nspname = '{schema_name}';
            "#,
        );

        let result: (i64,) = sqlx::query_as(&query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                errors::Errors::CountTableRowsFailed(format!(
                    "Failed to count rows in table {schema_name}.{table_name}: {e}"
                ))
            })?;

        Ok(result.0)
    }

    pub async fn peek_wal_changes(
        &self,
        publication_name: &str,
        replication_slot_name: &str,
        limit: u64, // recommendation: 65536
    ) -> errors::Result<Vec<PeekWalChangeResult>> {
        log::debug!(
            "Peeking WAL changes for publication: {publication_name}, slot: {replication_slot_name}, limit: {limit}"
        );

        let rows: Vec<PeekWalChangeResult> = sqlx::query_as(
            format!(r#"
                SELECT lsn::text as lsn, xid::text, data
		        FROM pg_logical_slot_peek_binary_changes('{replication_slot_name}', NULL, {limit}, 'proto_version', '1', 'publication_names', '{publication_name}')
            "#,
        )
        .as_str(),
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| {
            errors::Errors::PeekChangesFailed(format!("Failed to peek WAL changes: {e}"))
        })?;

        Ok(rows)
    }

    pub async fn advance_replication_slot(
        &self,
        replication_slot_name: &str,
        lsn: &str,
    ) -> errors::Result<()> {
        let query =
            format!("SELECT pg_replication_slot_advance('{replication_slot_name}', '{lsn}');");

        sqlx::query(&query).execute(&self.pool).await.map_err(|e| {
            errors::Errors::ReplicationSlotAdvanceFailed(format!(
                "Failed to advance replication slot: {e}"
            ))
        })?;

        Ok(())
    }

    /// COPY TO STDOUT을 사용하여 테이블 데이터를 바이트로 다운로드
    pub async fn copy_table_to_stdout(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> errors::Result<tokio::sync::mpsc::Receiver<Vec<PostgresCopyRow>>> {
        let query = format!("COPY (SELECT * FROM {schema_name}.{table_name}) TO STDOUT");

        log::debug!("Executing COPY TO STDOUT query: {query}");

        let connection_string = self.config.connection_string();

        // tokio-postgres를 사용하여 COPY TO STDOUT 실행
        let (client, connection) =
            tokio_postgres::connect(connection_string.as_str(), tokio_postgres::NoTls)
                .await
                .map_err(|e| {
                    errors::Errors::CopyTableFailed(format!(
                        "Failed to connect to PostgreSQL for COPY: {e}"
                    ))
                })?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                log::error!("Connection error: {e}");
            }
        });

        // COPY TO STDOUT 실행
        let copy_sink = client.copy_out(&query).await.map_err(|e| {
            errors::Errors::CopyTableFailed(format!(
                "Failed to start COPY TO STDOUT for table {table_name}: {e}"
            ))
        })?;

        let (sender, receiver) = tokio::sync::mpsc::channel(10000);

        let table_name = table_name.to_string();

        tokio::spawn(async move {
            // 스트림에서 직접 파싱하여 메모리 사용량 최적화
            use futures::StreamExt;

            let mut current_row = PostgresCopyRow {
                columns: Vec::new(),
            };
            let mut current_word = String::new();

            let mut stream: std::pin::Pin<Box<tokio_postgres::CopyOutStream>> = Box::pin(copy_sink);
            while let Some(chunk) = stream.next().await {
                let mut rows = Vec::new();

                let bytes = match chunk {
                    Ok(bytes) => bytes,
                    Err(error) => {
                        log::error!("Error reading COPY data for table {table_name}: {error}");
                        break;
                    }
                };

                // 바이트를 UTF-8 문자열로 변환하여 바로 파싱
                let text = unsafe { std::str::from_utf8_unchecked(&bytes) };
                let mut previous_was_escape = false;

                for c in text.chars() {
                    if previous_was_escape {
                        current_word.push(c);
                        previous_was_escape = false;
                        continue;
                    }

                    if c == '\\' {
                        current_word.push(c);
                        previous_was_escape = true;
                        continue;
                    }

                    // column separator
                    if c == '\t' {
                        current_row
                            .columns
                            .push(Self::finalize_copy_field(&mut current_word));
                        continue;
                    }

                    // row separator
                    if c == '\n' {
                        current_row
                            .columns
                            .push(Self::finalize_copy_field(&mut current_word));

                        rows.push(std::mem::take(&mut current_row));
                        continue;
                    }

                    current_word.push(c);
                }

                sender
                    .send(rows)
                    .await
                    .map_err(|e| {
                        errors::Errors::CopyTableFailed(format!(
                            "Failed to send copied rows for table {table_name}: {e}"
                        ))
                    })
                    .unwrap();
            }
        });

        Ok(receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresConnection;
    use crate::adapter::postgres::pgoutput::PgOutputValue;

    fn decode_copy_text_field_before_fix(input: &str) -> String {
        input.to_string()
    }

    #[test]
    fn decode_copy_text_field_before_and_after_json_escape_sequences() {
        let raw = r#"{"style_summary":"типа \\\"треугольник\\\""}"#;
        let before = decode_copy_text_field_before_fix(raw);
        let after = PostgresConnection::decode_copy_text_field(raw);

        assert_eq!(before, r#"{"style_summary":"типа \\\"треугольник\\\""}"#);
        assert_eq!(after, r#"{"style_summary":"типа \"треугольник\""}"#);
        assert_ne!(before, after);
    }

    #[test]
    fn decode_copy_text_field_before_and_after_control_characters() {
        let raw = r#"line1\nline2\tvalue\\path"#;
        let before = decode_copy_text_field_before_fix(raw);
        let after = PostgresConnection::decode_copy_text_field(raw);

        assert_eq!(before, r#"line1\nline2\tvalue\\path"#);
        assert_eq!(after, "line1\nline2\tvalue\\path");
        assert_ne!(before, after);
    }

    #[test]
    fn decode_copy_text_field_before_and_after_single_digit_hex_escape() {
        let raw = r#"prefix\xAsuffix"#;
        let before = decode_copy_text_field_before_fix(raw);
        let after = PostgresConnection::decode_copy_text_field(raw);

        assert_eq!(before, r#"prefix\xAsuffix"#);
        assert_eq!(after, "prefix\nsuffix");
        assert_ne!(before, after);
    }

    #[test]
    fn decode_copy_text_field_before_and_after_double_digit_hex_escape() {
        let raw = r#"prefix\x41suffix"#;
        let before = decode_copy_text_field_before_fix(raw);
        let after = PostgresConnection::decode_copy_text_field(raw);

        assert_eq!(before, r#"prefix\x41suffix"#);
        assert_eq!(after, "prefixAsuffix");
        assert_ne!(before, after);
    }

    #[test]
    fn decode_copy_text_field_keeps_null_sentinel_handling_separate() {
        let mut raw = r#"\N"#.to_string();
        let finalized = PostgresConnection::finalize_copy_field(&mut raw);

        assert!(matches!(finalized, PgOutputValue::Null));
        assert!(raw.is_empty());
    }
}
