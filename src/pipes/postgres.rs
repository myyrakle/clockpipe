use std::collections::HashMap;

use crate::{
    adapter::{
        self, IntoClickhouse,
        clickhouse::ClickhouseColumn,
        postgres::{
            PostgresColumn, PostgresCopyRow,
            pgoutput::{MessageType, PgOutputValue, parse_pg_output},
        },
    },
    config::{Configuraion, ToastFallback},
    errors::Errors,
    logger::ProgressLogger,
    pipes::{IPipe, WriteCounter},
};

#[derive(Debug, Clone, Default)]
pub struct PostgresTableRelation {
    pub schema_name: String,
    pub table_name: String,
}

#[derive(Debug, Clone, Default)]
pub struct PostgresPipeContext {
    tables_map: std::collections::HashMap<String, PostgresPipeTableInfo>,
    table_relation_map: std::collections::HashMap<u32, PostgresTableRelation>,
}

impl PostgresPipeContext {
    pub fn set_table(
        &mut self,
        schema_name: &str,
        table_name: &str,
        postgres_columns: Vec<PostgresColumn>,
        clickhouse_columns: Vec<ClickhouseColumn>,
    ) {
        self.tables_map.insert(
            format!("{schema_name}.{table_name}"),
            PostgresPipeTableInfo {
                postgres_columns,
                clickhouse_columns,
            },
        );
    }
}

#[derive(Debug, Clone)]
pub struct PostgresPipeTableInfo {
    postgres_columns: Vec<PostgresColumn>,
    clickhouse_columns: Vec<ClickhouseColumn>,
}

#[derive(Clone)]
pub struct PostgresPipe {
    context: PostgresPipeContext,

    config: Configuraion,

    postgres_config: crate::config::PostgresConfig,
    postgres_connection: adapter::postgres::PostgresConnection,

    clickhouse_config: crate::config::ClickHouseConfig,
    clickhouse_connection: adapter::clickhouse::ClickhouseConnection,
}

impl PostgresPipe {
    pub async fn new(
        config: Configuraion,
        postgres_config: crate::config::PostgresConfig,
        clickhouse_config: crate::config::ClickHouseConfig,
    ) -> Self {
        let postgres_connection =
            adapter::postgres::PostgresConnection::new(&postgres_config.connection)
                .await
                .expect("Failed to create Postgres connection");

        let clickhouse_connection =
            adapter::clickhouse::ClickhouseConnection::new(&clickhouse_config.connection);

        PostgresPipe {
            context: PostgresPipeContext::default(),
            config,
            postgres_config,
            clickhouse_config,
            postgres_connection,
            clickhouse_connection,
        }
    }
}

#[async_trait::async_trait]
impl IPipe for PostgresPipe {
    async fn ping(&self) -> Result<(), Errors> {
        self.postgres_connection
            .ping()
            .await
            .map_err(|e| Errors::DatabasePingError(format!("Postgres ping failed: {e}")))?;

        self.clickhouse_connection
            .ping()
            .await
            .map_err(|e| Errors::DatabasePingError(format!("ClickHouse ping failed: {e}")))?;

        log::info!("Postgres and ClickHouse connections are healthy.");

        Ok(())
    }

    async fn initialize(&mut self) {
        log::info!("Initializing Postgres Pipe...");

        self.setup_publication()
            .await
            .expect("Failed to setup Postgres Pipe");

        self.setup_table()
            .await
            .expect("Failed to setup ClickHouse table");
    }

    async fn first_sync(&self) {
        log::info!("Starting initial sync...");

        // 1. For each table in Postgres config
        for table in &self.postgres_config.tables {
            let schema_name = &table.schema_name;
            let table_name = &table.table_name;
            let mask_columns = &table.mask_columns;
            let source_table_info = self
                .context
                .tables_map
                .get(&format!("{schema_name}.{table_name}"))
                .expect("Table info not found in context");

            // 2. Check if skip_copy is set
            // If set, skip the initial sync for this table
            if table.skip_copy {
                log::debug!(
                    "Skipping initial sync for {schema_name}.{table_name} as skip_copy is set to true"
                );
                continue;
            }

            // 3. Check if table is not empty in ClickHouse
            // If not empty, skip the initial sync for this table
            if self
                .clickhouse_connection
                .table_is_not_empty(
                    &self.clickhouse_config.connection.database,
                    &table.table_name,
                )
                .await
                .expect("Failed to check if table exists")
            {
                log::info!(
                    "Table {schema_name}.{table_name} already exists in ClickHouse, skipping initial sync.",
                );
                continue;
            }

            // 4. get total row count in Postgres table (for progress logging only)
            let total_count =
                self.postgres_connection
                    .count_table_rows(schema_name, table_name)
                    .await
                    .expect("Failed to count table rows in Postgres") as usize;

            // 5. Start copying data from Postgres to ClickHouse
            log::info!(
                "Copying data from Postgres table {schema_name}.{table_name}... ({total_count} rows)",
            );
            let mut copy_receiver = self
                .postgres_connection
                .copy_table_to_stdout(&table.schema_name, &table.table_name)
                .await
                .expect("Failed to copy table data from Postgres");

            let mut processed_rows = 0_usize;
            let logger = ProgressLogger::new(
                &format!(
                    "Inserting copied data into ClickHouse table {schema_name}.{table_name}..."
                ),
                total_count,
            );

            // 6. Receive copied rows in batches and insert into ClickHouse
            let mut rows = Vec::new();
            while let Some(row_chunks) = copy_receiver.recv().await {
                rows.extend(row_chunks);

                // If buffer size is less than threshold, continue accumulating
                if rows.len() < self.config.copy_batch_size {
                    continue;
                }

                logger.log_progress(processed_rows);

                // 7. Do Insert into ClickHouse
                let insert_query = self.generate_insert_query(
                    &self.clickhouse_config,
                    &source_table_info.clickhouse_columns,
                    &source_table_info.postgres_columns,
                    mask_columns,
                    &table.table_name,
                    &rows,
                );

                if !insert_query.is_empty() {
                    self.clickhouse_connection
                        .execute_query(&insert_query)
                        .await
                        .expect("Failed to execute insert query in ClickHouse");
                }

                processed_rows += rows.len();
                rows.clear();
            }

            // Flush remaining rows that didn't reach the batch threshold
            if !rows.is_empty() {
                let insert_query = self.generate_insert_query(
                    &self.clickhouse_config,
                    &source_table_info.clickhouse_columns,
                    &source_table_info.postgres_columns,
                    mask_columns,
                    &table.table_name,
                    &rows,
                );

                if !insert_query.is_empty() {
                    self.clickhouse_connection
                        .execute_query(&insert_query)
                        .await
                        .expect("Failed to execute insert query in ClickHouse");
                }

                processed_rows += rows.len();
            }

            logger.clean();

            log::info!(
                "Copy completed for table {schema_name}.{table_name} ({processed_rows} rows)"
            );
        }
    }

    async fn sync_loop(&mut self) {
        if !self.clickhouse_config.enable_sync_loop() {
            log::info!("Sync loop disabled. Exiting...");
            return;
        }

        log::info!("Starting sync loop...");

        let publication_name = &self.postgres_config.publication_name;
        let replication_slot_name = &self.postgres_config.replication_slot_name;

        'SYNC_LOOP: loop {
            // 1. Peek new rows
            let peek_result = self
                .postgres_connection
                .peek_wal_changes(
                    publication_name,
                    replication_slot_name,
                    self.config.peek_changes_limit,
                )
                .await;

            let peek_result = match peek_result {
                Ok(peek) => peek,
                Err(e) => {
                    // Handle peek error. wait and retry
                    log::error!("Error peeking WAL changes: {e:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_when_peek_failed,
                    ))
                    .await;
                    continue;
                }
            };

            if peek_result.is_empty() {
                log::info!("No new changes found, waiting for next iteration...");
                tokio::time::sleep(std::time::Duration::from_millis(
                    self.config.sleep_millis_when_peek_is_empty,
                ))
                .await;
                continue 'SYNC_LOOP;
            }

            let mut table_log_map = HashMap::new();

            let mut batch_insert_queue = HashMap::new();
            let mut batch_delete_queue = HashMap::new();

            // 2. Parse peeked rows, group by table and prepare for insert/update/delete
            for row in peek_result.iter() {
                let parsed_row = match parse_pg_output(&row.data) {
                    Ok(Some(parsed)) => parsed,
                    Ok(None) => continue,
                    Err(e) => {
                        log::error!(
                            "Failed to parse PgOutput: {e:?}. Raw data (hex): {}",
                            row.data
                                .iter()
                                .map(|b| format!("{b:02x}"))
                                .collect::<Vec<_>>()
                                .join(" ")
                        );
                        panic!("Aborting due to PgOutput parse failure");
                    }
                };

                let Some(PostgresTableRelation {
                    schema_name,
                    table_name,
                }) = self.context.table_relation_map.get(&parsed_row.relation_id)
                else {
                    log::warn!(
                        "Relation ID {} not found in context table relation map",
                        parsed_row.relation_id
                    );
                    continue;
                };

                match parsed_row.message_type {
                    MessageType::Insert | MessageType::Update => {
                        let table_info = self
                            .context
                            .tables_map
                            .get(&format!("{schema_name}.{table_name}"))
                            .expect("Table info not found in context");

                        let mask_columns = self
                            .postgres_config
                            .tables
                            .iter()
                            .find(|t| {
                                t.table_name == table_name.as_str()
                                    && t.schema_name == schema_name.as_str()
                            })
                            .map_or_else(Vec::new, |t| t.mask_columns.clone());

                        batch_insert_queue
                            .entry(table_name)
                            .or_insert_with(|| BatchWriteEntry {
                                table_info,
                                mask_columns,
                                rows: Vec::new(),
                            })
                            .push(PostgresCopyRow {
                                columns: parsed_row.payload,
                            });

                        let count = table_log_map
                            .entry(format!("{schema_name}.{table_name}"))
                            .or_insert(WriteCounter::default());

                        if parsed_row.message_type == MessageType::Insert {
                            count.insert_count += 1;
                        } else {
                            count.update_count += 1;
                        }
                    }
                    MessageType::Delete => {
                        let source_table_info = self
                            .context
                            .tables_map
                            .get(&format!("{schema_name}.{table_name}"))
                            .expect("Table info not found in context");

                        batch_delete_queue
                            .entry(table_name)
                            .or_insert_with(|| BatchWriteEntry {
                                table_info: source_table_info,
                                mask_columns: Vec::new(),
                                rows: Vec::new(),
                            })
                            .push(PostgresCopyRow {
                                columns: parsed_row.payload,
                            });

                        let count = table_log_map
                            .entry(format!("{schema_name}.{table_name}"))
                            .or_insert(WriteCounter::default());

                        count.delete_count += 1;
                    }
                    MessageType::Truncate => {
                        // Truncate is handled separately, no need to queue

                        let database = &self.clickhouse_config.connection.database;

                        if let Err(error) = self
                            .clickhouse_connection
                            .truncate_table(database, table_name)
                            .await
                        {
                            log::error!(
                                "Failed to truncate table {}.{}: {}",
                                schema_name,
                                table_name,
                                error
                            );

                            tokio::time::sleep(std::time::Duration::from_millis(
                                self.config.sleep_millis_when_write_failed,
                            ))
                            .await;

                            continue 'SYNC_LOOP;
                        }

                        log::info!("Table {}.{} was truncated.", schema_name, table_name);
                    }
                    _ => {}
                }
            }

            // 2.5. Resolve leftover TOAST-Unchanged columns (batch-local + optional CH lookup)
            for (table_name, batch) in batch_insert_queue.iter_mut() {
                if let Err(error) = resolve_toast_unchanged(
                    &self.clickhouse_connection,
                    &self.clickhouse_config,
                    self.postgres_config.toast_fallback.as_ref(),
                    table_name,
                    batch,
                )
                .await
                {
                    log::error!("Failed to resolve TOAST for {table_name}: {error}");
                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_when_write_failed,
                    ))
                    .await;
                    continue 'SYNC_LOOP;
                }
            }

            // 3. Insert/Update rows in ClickHouse
            for (table_name, batch) in batch_insert_queue.iter() {
                let insert_query = self.generate_insert_query(
                    &self.clickhouse_config,
                    &batch.table_info.clickhouse_columns,
                    &batch.table_info.postgres_columns,
                    &batch.mask_columns,
                    table_name,
                    &batch.deduplicated_rows(),
                );

                if !insert_query.is_empty() {
                    if let Err(error) = self
                        .clickhouse_connection
                        .execute_query(&insert_query)
                        .await
                    {
                        log::error!("Failed to execute insert query for {table_name}: {error}");
                        tokio::time::sleep(std::time::Duration::from_millis(
                            self.config.sleep_millis_when_write_failed,
                        ))
                        .await;

                        continue 'SYNC_LOOP;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_after_sync_write,
                    ))
                    .await;
                }
            }

            // 4. Delete rows in ClickHouse
            for (table_name, batch) in batch_delete_queue.iter() {
                let delete_query = self.generate_delete_query(
                    &self.clickhouse_config,
                    &batch.table_info.clickhouse_columns,
                    &batch.table_info.postgres_columns,
                    table_name,
                    &batch.rows,
                );

                if !delete_query.is_empty() {
                    if let Err(error) = self
                        .clickhouse_connection
                        .execute_query(&delete_query)
                        .await
                    {
                        log::error!("Failed to execute delete query for {table_name}: {error}");
                        tokio::time::sleep(std::time::Duration::from_millis(
                            self.config.sleep_millis_when_write_failed,
                        ))
                        .await;

                        continue 'SYNC_LOOP;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_after_sync_write,
                    ))
                    .await;
                }
            }

            // 5. Move cursor for next peek
            if let Some(last) = peek_result.last() {
                let advance_key = &last.lsn;

                if let Err(e) = self
                    .postgres_connection
                    .advance_replication_slot(replication_slot_name, advance_key)
                    .await
                {
                    log::error!("Error advancing exporter: {e:?}");
                    continue 'SYNC_LOOP;
                }
            }

            // 6. Log the changes
            for (table_name, count) in table_log_map.iter() {
                log::info!(
                    "Table [{}]: Inserted: {}, Updated: {}, Deleted: {}",
                    table_name,
                    count.insert_count,
                    count.update_count,
                    count.delete_count
                );
            }

            tokio::time::sleep(std::time::Duration::from_millis(
                self.config.sleep_millis_after_sync_iteration,
            ))
            .await;
        }
    }
}

impl PostgresPipe {
    async fn setup_publication(&self) -> Result<(), Errors> {
        if !self.clickhouse_config.enable_sync_loop() {
            log::info!("Sync loop disabled. Not setting up publication and replication slot.");
            return Ok(());
        }

        log::info!("Setup publication and replication slot...");

        let publication_name = &self.postgres_config.publication_name;

        // 1. Publication Create Step
        let publication = self
            .postgres_connection
            .find_publication_by_name(publication_name)
            .await?;

        if publication.is_none() {
            log::info!("Publication {publication_name} does not exist, creating a new one");

            let source_tables: Vec<String> = self
                .postgres_config
                .tables
                .iter()
                .map(|table| format!("{}.{}", table.schema_name, table.table_name))
                .collect();

            if source_tables.is_empty() {
                return Err(Errors::PublicationCreateFailed(
                    "No source tables specified in Postgres configuration".to_string(),
                ));
            }

            log::debug!("Source Tables: {source_tables:?}");

            self.postgres_connection
                .create_publication(publication_name, &source_tables)
                .await?;

            log::info!("Publication {publication_name} created successfully");
        } else {
            log::info!("Publication {publication_name} already exists, skipping creation.");
        }

        // 2. Publication Tables Add Step
        log::info!("Checking and adding tables to publication...");

        let publication_tables = self
            .postgres_connection
            .get_publication_tables(publication_name)
            .await?;

        for table in &self.postgres_config.tables {
            let table_name = format!("{}.{}", table.schema_name, table.table_name);

            if !publication_tables
                .iter()
                .any(|t| t.table_name == table.table_name && t.schema_name == table.schema_name)
            {
                log::info!("Adding table {table_name} to publication");
                self.postgres_connection
                    .add_table_to_publication(publication_name, &[&table_name])
                    .await?;
                log::info!("Table {table_name} added to publication");

                continue;
            }
        }

        // 3. Replication Slot Create Step
        log::info!("Setup Replication Slot...");

        let replication_slot_name = &self.postgres_config.replication_slot_name;

        let replication_slot = self
            .postgres_connection
            .find_replication_slot_by_name(replication_slot_name)
            .await?;

        if replication_slot.is_none() {
            log::info!(
                "Replication slot {replication_slot_name} does not exist, creating a new one"
            );

            self.postgres_connection
                .create_replication_slot(replication_slot_name)
                .await?;

            log::info!("Replication slot {replication_slot_name} created successfully");
        }

        Ok(())
    }

    async fn setup_table(&mut self) -> Result<(), Errors> {
        log::info!("Setting up tables in ClickHouse...");

        for table in &self.postgres_config.tables {
            let clickhouse_table_not_exists = self
                .clickhouse_connection
                .list_columns_by_tablename(
                    &self.clickhouse_config.connection.database,
                    &table.table_name,
                )
                .await?
                .is_empty();

            let postgres_columns = self
                .postgres_connection
                .list_columns_by_tablename(&table.schema_name, &table.table_name)
                .await?;

            let table_comment = self
                .postgres_connection
                .get_comment_from_table(&table.schema_name, &table.table_name)
                .await?;

            if clickhouse_table_not_exists {
                log::info!(
                    "Table {}.{} does not exist in ClickHouse, creating it",
                    table.schema_name,
                    table.table_name
                );

                let mut table_options = table.table_options.clone();
                table_options.inherit_from(&self.clickhouse_config.table_options);

                let create_table_query = self.generate_create_table_query(
                    &self.clickhouse_config,
                    &table_options,
                    &table.table_name,
                    &postgres_columns,
                    &table_comment,
                );

                self.clickhouse_connection
                    .execute_query(&create_table_query)
                    .await?;

                log::info!(
                    "Table {}.{} created in ClickHouse",
                    table.schema_name,
                    table.table_name
                );
            }

            let relation_id = self
                .postgres_connection
                .get_relation_id_by_table_name(&table.schema_name, &table.table_name)
                .await?;

            let mut clickhouse_columns = self
                .clickhouse_connection
                .list_columns_by_tablename(
                    &self.clickhouse_config.connection.database,
                    &table.table_name,
                )
                .await?;

            // Check if all Postgres columns exist in ClickHouse
            let mut need_refresh_columns = false;

            for postgres_column in &postgres_columns {
                if !clickhouse_columns
                    .iter()
                    .any(|c| c.column_name == postgres_column.column_name)
                {
                    log::info!(
                        "[{}.{}] Column {} does not exist in ClickHouse. Try to add it",
                        table.schema_name,
                        table.table_name,
                        postgres_column.column_name,
                    );

                    let add_column_query = self.generate_add_column_query(
                        &self.clickhouse_config,
                        table.table_name.as_str(),
                        postgres_column,
                    );

                    self.clickhouse_connection
                        .execute_query(&add_column_query)
                        .await?;

                    log::info!(
                        "[{}.{}] Column {} added to ClickHouse",
                        table.schema_name,
                        table.table_name,
                        postgres_column.column_name,
                    );

                    need_refresh_columns = true;

                    continue;
                }
            }

            if need_refresh_columns {
                clickhouse_columns = self
                    .clickhouse_connection
                    .list_columns_by_tablename(
                        &self.clickhouse_config.connection.database,
                        &table.table_name,
                    )
                    .await?;
            }

            self.context.set_table(
                table.schema_name.as_str(),
                table.table_name.as_str(),
                postgres_columns,
                clickhouse_columns,
            );
            self.context.table_relation_map.insert(
                relation_id as u32,
                PostgresTableRelation {
                    schema_name: table.schema_name.clone(),
                    table_name: table.table_name.clone(),
                },
            );
        }

        Ok(())
    }
}

impl IntoClickhouse for PostgresPipe {}

pub async fn run_postgres_pipe(config: Configuraion) {
    let mut pipe = PostgresPipe::new(
        config.clone(),
        config.source.postgres.expect("Postgres config is required"),
        config
            .target
            .clickhouse
            .expect("Clickhouse config is required"),
    )
    .await;

    if let Err(error) = pipe.ping().await {
        log::error!("Failed to ping Postgres exporter: {error:?}");
        return;
    }

    tokio::select! {
        _ = pipe.run_pipe() => {
            log::info!("Postgres pipe running...");
        }
    }
}

/// Maximum tuples per generated `IN` list. Keeps SQL well below ClickHouse's
/// default `max_query_size` (256 KiB) even with wide composite PKs.
const TOAST_LOOKUP_CHUNK_SIZE: usize = 1000;

async fn resolve_toast_unchanged(
    ch_connection: &adapter::clickhouse::ClickhouseConnection,
    ch_config: &crate::config::ClickHouseConfig,
    fallback: Option<&ToastFallback>,
    table_name: &str,
    batch: &mut BatchWriteEntry<'_>,
) -> Result<(), Errors> {
    let pg_columns = &batch.table_info.postgres_columns;
    let ch_columns = &batch.table_info.clickhouse_columns;

    let pk_row_indexes: Vec<usize> = pg_columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(|c| (c.column_index - 1) as usize)
        .collect();

    // Parallel flags: should the k-th PK column be normalized via
    // `format_date_time` so pgoutput text (`2025-08-18 05:16:08.4+00`) matches
    // CH `toString(DateTime)` (`2025-08-18 05:16:08`) during key comparison.
    let pk_normalize_temporal: Vec<bool> = pg_columns
        .iter()
        .filter(|c| c.is_primary_key)
        .map(is_temporal_pg_type)
        .collect();

    // If there is no primary key we cannot uniquely look up rows —
    // batch-local fill is still meaningful only when we can key by something,
    // so skip and let final NULL-fallback run.
    if pk_row_indexes.is_empty() {
        null_fill_leftovers(&mut batch.rows, table_name);
        return Ok(());
    }

    batch_local_fill(&mut batch.rows, &pk_row_indexes, &pk_normalize_temporal);

    let still_unresolved = batch
        .rows
        .iter()
        .any(|r| r.columns.iter().any(|v| matches!(v, PgOutputValue::Unchanged)));

    if still_unresolved && matches!(fallback, Some(ToastFallback::Lookup)) {
        clickhouse_fill(
            ch_connection,
            ch_config,
            table_name,
            pg_columns,
            ch_columns,
            &pk_row_indexes,
            &pk_normalize_temporal,
            &mut batch.rows,
        )
        .await?;
    }

    null_fill_leftovers(&mut batch.rows, table_name);
    Ok(())
}

fn batch_local_fill(
    rows: &mut [PostgresCopyRow],
    pk_row_indexes: &[usize],
    pk_normalize_temporal: &[bool],
) {
    let mut last_by_pk: HashMap<Vec<String>, Vec<PgOutputValue>> = HashMap::new();
    for row in rows.iter_mut() {
        let pk_key = pk_signature(row, pk_row_indexes, pk_normalize_temporal);
        if let Some(prev) = last_by_pk.get(&pk_key) {
            for (j, value) in row.columns.iter_mut().enumerate() {
                if matches!(value, PgOutputValue::Unchanged)
                    && let Some(prev_v) = prev.get(j)
                    && !matches!(prev_v, PgOutputValue::Unchanged)
                {
                    *value = prev_v.clone();
                }
            }
        }
        last_by_pk.insert(pk_key, row.columns.clone());
    }
}

fn pk_signature(
    row: &PostgresCopyRow,
    pk_row_indexes: &[usize],
    pk_normalize_temporal: &[bool],
) -> Vec<String> {
    pk_row_indexes
        .iter()
        .enumerate()
        .map(|(k, &i)| {
            let normalize = pk_normalize_temporal.get(k).copied().unwrap_or(false);
            match row.columns.get(i) {
                Some(PgOutputValue::Text(s)) => {
                    let v = if normalize {
                        PgOutputValue::format_date_time(s)
                    } else {
                        s.clone()
                    };
                    format!("T:{v}")
                }
                Some(PgOutputValue::Null) => "N:".to_string(),
                Some(PgOutputValue::Binary(b)) => format!("B:{}", hex_lower(b)),
                Some(PgOutputValue::Unchanged) => "U:".to_string(),
                Some(PgOutputValue::ClickhouseLiteral(s)) => format!("L:{s}"),
                Some(PgOutputValue::Unit) | None => "-:".to_string(),
            }
        })
        .collect()
}

/// Produces the same key shape as `pk_signature` from TSV cells returned by
/// ClickHouse (`toString(pk)` columns). Temporal columns pass through
/// `format_date_time` so keys match pgoutput-sourced signatures.
fn pk_signature_from_tsv(cells: &[Option<String>], pk_normalize_temporal: &[bool]) -> Vec<String> {
    cells
        .iter()
        .enumerate()
        .map(|(k, cell)| {
            let normalize = pk_normalize_temporal.get(k).copied().unwrap_or(false);
            match cell {
                Some(s) => {
                    let v = if normalize {
                        PgOutputValue::format_date_time(s)
                    } else {
                        s.clone()
                    };
                    format!("T:{v}")
                }
                None => "N:".to_string(),
            }
        })
        .collect()
}

fn is_temporal_pg_type(column: &PostgresColumn) -> bool {
    matches!(column.data_type.as_str(), "timestamp" | "timestamptz")
}

fn is_clickhouse_array_type(data_type: &str) -> bool {
    data_type.starts_with("Array(") || data_type.starts_with("Nullable(Array(")
}

fn hex_lower(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn null_fill_leftovers(rows: &mut [PostgresCopyRow], table_name: &str) {
    use std::collections::BTreeMap;
    let mut col_counts: BTreeMap<usize, usize> = BTreeMap::new();
    let mut affected_rows = 0_usize;
    for row in rows.iter_mut() {
        let mut row_had_leftover = false;
        for (j, value) in row.columns.iter_mut().enumerate() {
            if matches!(value, PgOutputValue::Unchanged) {
                *value = PgOutputValue::Null;
                *col_counts.entry(j).or_insert(0) += 1;
                row_had_leftover = true;
            }
        }
        if row_had_leftover {
            affected_rows += 1;
        }
    }
    if affected_rows > 0 {
        log::warn!(
            "TOAST: {affected_rows} row(s) in table {table_name} had Unchanged columns that could not be resolved; filled with NULL. Per-column counts: {col_counts:?}. Consider enabling REPLICA IDENTITY FULL or `toast_fallback: lookup`."
        );
    }
}

#[allow(clippy::too_many_arguments)]
async fn clickhouse_fill(
    ch_connection: &adapter::clickhouse::ClickhouseConnection,
    ch_config: &crate::config::ClickHouseConfig,
    table_name: &str,
    pg_columns: &[PostgresColumn],
    ch_columns: &[ClickhouseColumn],
    pk_row_indexes: &[usize],
    pk_normalize_temporal: &[bool],
    rows: &mut [PostgresCopyRow],
) -> Result<(), Errors> {
    // Union of column indexes still Unchanged across the batch.
    let mut unresolved_col_indexes: std::collections::BTreeSet<usize> = Default::default();
    for row in rows.iter() {
        for (j, v) in row.columns.iter().enumerate() {
            if matches!(v, PgOutputValue::Unchanged) {
                unresolved_col_indexes.insert(j);
            }
        }
    }
    if unresolved_col_indexes.is_empty() {
        return Ok(());
    }

    // Map row-array index -> column name (source Postgres columns are 1-indexed on column_index).
    let mut col_name_by_row_idx: HashMap<usize, &str> = HashMap::new();
    for c in pg_columns {
        col_name_by_row_idx.insert((c.column_index - 1) as usize, c.column_name.as_str());
    }

    let pk_column_names: Vec<&str> = pk_row_indexes
        .iter()
        .map(|i| *col_name_by_row_idx.get(i).unwrap_or(&""))
        .collect();
    if pk_column_names.iter().any(|n| n.is_empty()) {
        return Err(Errors::DatabaseQueryError(format!(
            "TOAST resolver: primary key column name missing for {table_name}"
        )));
    }

    // Collect distinct PK tuples that still have unresolved columns.
    let mut distinct_pks: HashMap<Vec<String>, Vec<PgOutputValue>> = HashMap::new();
    for row in rows.iter() {
        let has_unchanged = row
            .columns
            .iter()
            .any(|v| matches!(v, PgOutputValue::Unchanged));
        if !has_unchanged {
            continue;
        }
        let key = pk_signature(row, pk_row_indexes, pk_normalize_temporal);
        if distinct_pks.contains_key(&key) {
            continue;
        }
        let pk_values: Vec<PgOutputValue> = pk_row_indexes
            .iter()
            .map(|&i| row.columns.get(i).cloned().unwrap_or(PgOutputValue::Null))
            .collect();
        distinct_pks.insert(key, pk_values);
    }
    if distinct_pks.is_empty() {
        return Ok(());
    }

    // Resolve CH columns for PKs (used to render typed literals in the IN list).
    let mut pk_ch_columns: Vec<&ClickhouseColumn> = Vec::with_capacity(pk_column_names.len());
    for name in &pk_column_names {
        let Some(c) = ch_columns.iter().find(|c| c.column_name.as_str() == *name) else {
            return Err(Errors::DatabaseQueryError(format!(
                "TOAST resolver: primary key column `{name}` not found in ClickHouse table {table_name}"
            )));
        };
        pk_ch_columns.push(c);
    }

    // Ordered list of columns to SELECT: PK first, then unresolved columns.
    let missing_col_names: Vec<&str> = unresolved_col_indexes
        .iter()
        .map(|i| *col_name_by_row_idx.get(i).unwrap_or(&""))
        .collect();
    if missing_col_names.iter().any(|n| n.is_empty()) {
        return Err(Errors::DatabaseQueryError(format!(
            "TOAST resolver: source column name missing for {table_name}"
        )));
    }

    // Detect array-typed missing columns so fetched (CH-formatted) literals can
    // be emitted verbatim on re-INSERT.
    let missing_is_array: Vec<bool> = missing_col_names
        .iter()
        .map(|name| {
            ch_columns
                .iter()
                .find(|c| c.column_name.as_str() == *name)
                .map(|c| is_clickhouse_array_type(&c.data_type))
                .unwrap_or(false)
        })
        .collect();

    let select_expressions: Vec<String> = pk_column_names
        .iter()
        .chain(missing_col_names.iter())
        .map(|n| format!("toString(`{n}`) AS `{n}`"))
        .collect();

    let pk_tuple_expr = pk_column_names
        .iter()
        .map(|n| format!("`{n}`"))
        .collect::<Vec<_>>()
        .join(", ");

    // Render tuple literals once (shared across chunks).
    let mut keyed_tuples: Vec<(Vec<String>, String)> = Vec::with_capacity(distinct_pks.len());
    for (key, pk_values) in distinct_pks {
        let parts: Vec<String> = pk_values
            .iter()
            .enumerate()
            .map(|(i, v)| pk_ch_columns[i].to_clickhouse_value(v.clone()))
            .collect();
        keyed_tuples.push((key, format!("({})", parts.join(", "))));
    }

    let pk_count = pk_column_names.len();
    let mut fetched_by_pk: HashMap<Vec<String>, HashMap<usize, PgOutputValue>> = HashMap::new();

    // Chunk the IN list so one SELECT stays well below CH's `max_query_size`.
    for chunk in keyed_tuples.chunks(TOAST_LOOKUP_CHUNK_SIZE) {
        let tuples_sql = chunk
            .iter()
            .map(|(_, lit)| lit.as_str())
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "SELECT {select} FROM {db}.{table} FINAL WHERE ({pk_tuple}) IN ({tuples})",
            select = select_expressions.join(", "),
            db = ch_config.connection.database,
            table = table_name,
            pk_tuple = pk_tuple_expr,
            tuples = tuples_sql,
        );

        let fetched = ch_connection.fetch_string_rows(&query).await?;

        for row in fetched {
            if row.len() < pk_count {
                continue;
            }
            let pk_cells = &row[..pk_count];
            let key = pk_signature_from_tsv(pk_cells, pk_normalize_temporal);
            let mut col_map: HashMap<usize, PgOutputValue> = HashMap::new();
            for (offset, missing_idx) in unresolved_col_indexes.iter().enumerate() {
                let cell = row.get(pk_count + offset).and_then(Clone::clone);
                let value = match cell {
                    None => PgOutputValue::Null,
                    Some(s) if missing_is_array[offset] => PgOutputValue::ClickhouseLiteral(s),
                    Some(s) => PgOutputValue::Text(s),
                };
                col_map.insert(*missing_idx, value);
            }
            fetched_by_pk.insert(key, col_map);
        }
    }

    // Fill each row's Unchanged from fetched values.
    for row in rows.iter_mut() {
        let key = pk_signature(row, pk_row_indexes, pk_normalize_temporal);
        let Some(col_map) = fetched_by_pk.get(&key) else {
            continue;
        };
        for (j, value) in row.columns.iter_mut().enumerate() {
            if matches!(value, PgOutputValue::Unchanged)
                && let Some(v) = col_map.get(&j)
            {
                *value = v.clone();
            }
        }
    }

    Ok(())
}

pub struct BatchWriteEntry<'a> {
    pub table_info: &'a PostgresPipeTableInfo,
    pub mask_columns: Vec<String>,
    pub rows: Vec<PostgresCopyRow>,
}

impl BatchWriteEntry<'_> {
    pub fn push(&mut self, row: PostgresCopyRow) {
        self.rows.push(row);
    }

    pub fn deduplicated_rows(&self) -> Vec<PostgresCopyRow> {
        adapter::deduplicate_rows_keeping_last(self.rows.clone(), |row| {
            extract_postgres_primary_key(row, &self.table_info.postgres_columns)
        })
    }
}

fn extract_postgres_primary_key(row: &PostgresCopyRow, columns: &[PostgresColumn]) -> String {
    columns
        .iter()
        .filter(|col| col.is_primary_key)
        .map(|col| {
            let index = (col.column_index - 1) as usize;
            match row.columns.get(index) {
                Some(value) => format!("{value:?}"),
                None => "NULL".to_string(),
            }
        })
        .collect::<Vec<_>>()
        .join("|")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(s: &str) -> PgOutputValue {
        PgOutputValue::Text(s.to_string())
    }

    fn row(cols: Vec<PgOutputValue>) -> PostgresCopyRow {
        PostgresCopyRow { columns: cols }
    }

    #[test]
    fn is_clickhouse_array_type_matches_array_and_nullable_array() {
        assert!(is_clickhouse_array_type("Array(Int32)"));
        assert!(is_clickhouse_array_type("Array(String)"));
        assert!(is_clickhouse_array_type("Nullable(Array(String))"));
        assert!(!is_clickhouse_array_type("String"));
        assert!(!is_clickhouse_array_type("Nullable(String)"));
        assert!(!is_clickhouse_array_type("ArrayLike"));
    }

    #[test]
    fn pk_signature_distinguishes_pipe_colliding_pairs() {
        // Two-column PK, both Text. If pk_signature were a joined string with
        // "|" as the separator, these two rows would collide.
        let a = row(vec![text("a|T:b"), text("c")]);
        let b = row(vec![text("a"), text("b|T:c")]);
        let indexes = [0usize, 1usize];
        let normalize = [false, false];
        assert_ne!(
            pk_signature(&a, &indexes, &normalize),
            pk_signature(&b, &indexes, &normalize)
        );
    }

    #[test]
    fn pk_signature_normalizes_temporal_columns() {
        let r = row(vec![text("2025-08-18 05:16:08.490845+00")]);
        let indexes = [0usize];
        let temporal = pk_signature(&r, &indexes, &[true]);
        let raw = pk_signature(&r, &indexes, &[false]);
        assert_eq!(temporal, vec!["T:2025-08-18 05:16:08".to_string()]);
        assert_eq!(
            raw,
            vec!["T:2025-08-18 05:16:08.490845+00".to_string()]
        );
    }

    #[test]
    fn pk_signature_from_tsv_matches_pgoutput_signature_for_datetime() {
        // CH toString(DateTime) emits already-normalized form; pgoutput sends
        // the fractional+timezone form. Both must produce the same key.
        let pg_row = row(vec![text("2025-08-18 05:16:08.490845+00")]);
        let indexes = [0usize];
        let normalize = [true];
        let from_pg = pk_signature(&pg_row, &indexes, &normalize);
        let from_ch = pk_signature_from_tsv(
            &[Some("2025-08-18 05:16:08".to_string())],
            &normalize,
        );
        assert_eq!(from_pg, from_ch);
    }

    #[test]
    fn batch_local_fill_carries_prior_full_row_into_toast_update() {
        // Columns: [id (pk), big_text]
        // Row 1: INSERT {id=1, big_text="hello"}
        // Row 2: UPDATE {id=1, big_text=Unchanged}  -> should fill "hello"
        let mut rows = vec![
            row(vec![text("1"), text("hello")]),
            row(vec![text("1"), PgOutputValue::Unchanged]),
        ];
        batch_local_fill(&mut rows, &[0], &[false]);
        assert!(matches!(&rows[1].columns[1], PgOutputValue::Text(s) if s == "hello"));
    }

    #[test]
    fn batch_local_fill_leaves_unchanged_when_no_prior_row() {
        let mut rows = vec![row(vec![text("1"), PgOutputValue::Unchanged])];
        batch_local_fill(&mut rows, &[0], &[false]);
        assert!(matches!(rows[0].columns[1], PgOutputValue::Unchanged));
    }

    #[test]
    fn batch_local_fill_does_not_cross_pks() {
        let mut rows = vec![
            row(vec![text("1"), text("one")]),
            row(vec![text("2"), PgOutputValue::Unchanged]),
        ];
        batch_local_fill(&mut rows, &[0], &[false]);
        // PK 2 should not inherit "one" from PK 1.
        assert!(matches!(rows[1].columns[1], PgOutputValue::Unchanged));
    }

    #[test]
    fn batch_local_fill_chains_through_multiple_updates() {
        // Row 1: INSERT val="v1"
        // Row 2: UPDATE val="v2"
        // Row 3: UPDATE val=Unchanged  -> should fill from row 2 ("v2"), not row 1
        let mut rows = vec![
            row(vec![text("1"), text("v1")]),
            row(vec![text("1"), text("v2")]),
            row(vec![text("1"), PgOutputValue::Unchanged]),
        ];
        batch_local_fill(&mut rows, &[0], &[false]);
        assert!(matches!(&rows[2].columns[1], PgOutputValue::Text(s) if s == "v2"));
    }
}
