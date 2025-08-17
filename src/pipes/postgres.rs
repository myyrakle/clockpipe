use std::collections::HashMap;

use crate::{
    adapter::{
        self, IntoClickhouse,
        clickhouse::ClickhouseColumn,
        postgres::{
            PostgresColumn, PostgresCopyRow,
            pgoutput::{MessageType, parse_pg_output},
        },
    },
    config::Configuraion,
    errors::Errors,
    pipes::IPipe,
};

#[derive(Debug, Clone, Default)]
pub struct PostgresPipeContext {
    tables_map: std::collections::HashMap<String, PostgresPipeTableInfo>,
    table_relation_map: std::collections::HashMap<u32, (String, String)>,
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

    postgres_config: crate::config::PostgresConfig,
    postgres_connection: adapter::postgres::PostgresConnection,

    clickhouse_config: crate::config::ClickHouseConfig,
    clickhouse_connection: adapter::clickhouse::ClickhouseConnection,
}

impl PostgresPipe {
    pub async fn new(
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

        for table in &self.postgres_config.tables {
            let schema_name = &table.schema_name;
            let table_name = &table.table_name;

            if table.skip_copy {
                log::debug!(
                    "Skipping initial sync for {schema_name}.{table_name} as skip_copy is set to true"
                );
                continue;
            }

            if self
                .clickhouse_connection
                .table_is_not_empty(
                    &self.clickhouse_config.connection.database,
                    &table.table_name,
                )
                .await
                .expect("Failed to check if table exists")
            {
                log::debug!(
                    "Table {schema_name}.{table_name} already exists in ClickHouse, skipping initial sync.",
                );
                continue;
            }

            log::info!("Copying data from Postgres table {schema_name}.{table_name}...",);
            let rows = self
                .postgres_connection
                .copy_table_to_stdout(&table.schema_name, &table.table_name)
                .await
                .expect("Failed to copy table data from Postgres");

            log::info!("Inserting copied data into ClickHouse table {schema_name}.{table_name}...",);

            let source_table_info = self
                .context
                .tables_map
                .get(&format!("{schema_name}.{table_name}"))
                .expect("Table info not found in context");

            let chunks = rows.chunks(100000);
            let chunk_count = chunks.len();

            for (chunk_index, chunk) in chunks.enumerate() {
                let chunk_index = chunk_index + 1;
                let percent = (chunk_index * 100) / chunk_count;

                log::debug!(
                    "Processing chunk {percent}% ({chunk_index}/{chunk_count}) for table {schema_name}.{table_name}",
                );

                let insert_query = self.generate_insert_query(
                    &self.clickhouse_config,
                    &source_table_info.clickhouse_columns,
                    &source_table_info.postgres_columns,
                    &table.table_name,
                    chunk,
                );

                if !insert_query.is_empty() {
                    self.clickhouse_connection
                        .execute_query(&insert_query)
                        .await
                        .expect("Failed to execute insert query in ClickHouse");
                }
            }

            log::info!("Copy completed for table {schema_name}.{table_name}");
        }
    }

    async fn sync_loop(&self) {
        log::info!("Starting sync loop...");

        let publication_name = &self.postgres_config.publication_name;
        let replication_slot_name = &self.postgres_config.replication_slot_name;

        loop {
            // 1. Peek new rows
            let peek_result = self
                .postgres_connection
                .peek_wal_changes(
                    publication_name,
                    replication_slot_name,
                    self.postgres_config.peek_changes_limit,
                )
                .await;

            let peek_result = match peek_result {
                Ok(peek) => peek,
                Err(e) => {
                    // 1.1. Handle peek error. wait and retry
                    log::error!("Error peeking WAL changes: {e:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.postgres_config.sleep_millis_when_peek_failed,
                    ))
                    .await;
                    continue;
                }
            };

            // parse peeked rows

            if peek_result.is_empty() {
                log::info!("No new changes found, waiting for next iteration...");
                tokio::time::sleep(std::time::Duration::from_millis(
                    self.postgres_config.sleep_millis_when_peek_is_empty,
                ))
                .await;
                continue;
            }

            pub struct Count {
                pub insert_count: usize,
                pub update_count: usize,
                pub delete_count: usize,
            }
            let mut table_log_map = HashMap::new();

            pub struct InsertBatch<'a> {
                pub table_info: &'a PostgresPipeTableInfo,
                pub rows: Vec<PostgresCopyRow>,
            }

            impl InsertBatch<'_> {
                pub fn push(&mut self, row: PostgresCopyRow) {
                    self.rows.push(row);
                }
            }

            let mut batch_insert_queue = HashMap::new();

            for row in peek_result.iter() {
                let Some(parsed_row) =
                    parse_pg_output(&row.data).expect("Failed to parse PgOutput")
                else {
                    continue;
                };

                let Some((schema_name, table_name)) =
                    self.context.table_relation_map.get(&parsed_row.relation_id)
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

                        batch_insert_queue
                            .entry(table_name)
                            .or_insert_with(|| InsertBatch {
                                table_info,
                                rows: Vec::new(),
                            })
                            .push(PostgresCopyRow {
                                columns: parsed_row.payload,
                            });

                        let count = table_log_map
                            .entry(format!("{schema_name}.{table_name}"))
                            .or_insert(Count {
                                insert_count: 0,
                                update_count: 0,
                                delete_count: 0,
                            });

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

                        let delete_query = self.generate_delete_query(
                            &self.clickhouse_config,
                            &source_table_info.clickhouse_columns,
                            &source_table_info.postgres_columns,
                            table_name,
                            &PostgresCopyRow {
                                columns: parsed_row.payload,
                            },
                        );

                        if let Err(error) = self
                            .clickhouse_connection
                            .execute_query(&delete_query)
                            .await
                        {
                            log::error!(
                                "Failed to execute delete query for {schema_name}.{table_name}: {error}"
                            );
                            tokio::time::sleep(std::time::Duration::from_millis(
                                self.postgres_config.sleep_millis_when_write_failed,
                            ))
                            .await;

                            continue;
                        }

                        let count = table_log_map
                            .entry(format!("{schema_name}.{table_name}"))
                            .or_insert(Count {
                                insert_count: 0,
                                update_count: 0,
                                delete_count: 0,
                            });

                        count.delete_count += 1;
                    }
                    _ => {}
                }
            }

            for (table_name, batch) in batch_insert_queue.iter() {
                let insert_query = self.generate_insert_query(
                    &self.clickhouse_config,
                    &batch.table_info.clickhouse_columns,
                    &batch.table_info.postgres_columns,
                    table_name,
                    &batch.rows,
                );

                if !insert_query.is_empty() {
                    if let Err(error) = self
                        .clickhouse_connection
                        .execute_query(&insert_query)
                        .await
                    {
                        log::error!("Failed to execute insert query for {table_name}: {error}");
                        tokio::time::sleep(std::time::Duration::from_millis(
                            self.postgres_config.sleep_millis_when_write_failed,
                        ))
                        .await;

                        continue;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                }
            }

            // Advance the exporter
            if let Some(last) = peek_result.last() {
                let advance_key = &last.lsn;

                if let Err(e) = self
                    .postgres_connection
                    .advance_replication_slot(replication_slot_name, advance_key)
                    .await
                {
                    log::error!("Error advancing exporter: {e:?}");
                    continue;
                }
            }

            // Log the changes
            for (table_name, count) in table_log_map.iter() {
                log::info!(
                    "Table [{}]: Inserted: {}, Updated: {}, Deleted: {}",
                    table_name,
                    count.insert_count,
                    count.update_count,
                    count.delete_count
                );
            }

            tokio::time::sleep(std::time::Duration::from_millis(500)).await
        }
    }
}

impl PostgresPipe {
    async fn setup_publication(&self) -> Result<(), Errors> {
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

            if clickhouse_table_not_exists {
                log::info!(
                    "Table {}.{} does not exist in ClickHouse, creating it",
                    table.schema_name,
                    table.table_name
                );
                let create_table_query = self.generate_create_table_query(
                    &self.clickhouse_config.connection.database,
                    &table.table_name,
                    &postgres_columns,
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
                (table.schema_name.clone(), table.table_name.clone()),
            );
        }

        Ok(())
    }
}

impl IntoClickhouse for PostgresPipe {}

pub async fn run_postgres_pipe(config: Configuraion) {
    let mut pipe = PostgresPipe::new(
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
