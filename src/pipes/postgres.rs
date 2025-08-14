use std::collections::HashMap;

use crate::{
    adapter::{
        self,
        clickhouse::ClickhouseColumn,
        convert::IntoClickhouse,
        postgres::{
            PostgresColumn, PostgresCopyRow,
            pgoutput::{MessageType, parse_pg_output},
        },
    },
    config::Configuraion,
    errors::Errors,
    interface::IPipe,
};

#[derive(Debug, Clone, Default)]
pub struct PostgresPipeContext {
    pub tables_map: std::collections::HashMap<String, PostgresPipeTableInfo>,
    pub table_relation_map: std::collections::HashMap<u32, (String, String)>,
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
    pub postgres_columns: Vec<PostgresColumn>,
    pub clickhouse_columns: Vec<ClickhouseColumn>,
}

#[derive(Clone)]
pub struct PostgresPipe {
    pub context: PostgresPipeContext,

    pub postgres_config: crate::config::PostgresConfig,
    pub clickhouse_config: crate::config::ClickHouseConfig,
    postgres_connection: adapter::postgres::PostgresConnection,
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

    async fn run_pipe(&mut self) {
        self.initialize().await;

        self.first_sync().await;
        self.sync_loop().await;
    }
}

impl PostgresPipe {
    async fn initialize(&mut self) {
        log::info!("Initializing Postgres exporter...");

        log::info!("Setup publication and replication slot");
        self.setup_publication()
            .await
            .expect("Failed to setup exporter");

        log::info!("Setup ClickHouse table");
        self.setup_table()
            .await
            .expect("Failed to setup ClickHouse table");
    }

    async fn setup_publication(&self) -> Result<(), Errors> {
        // 1. Publication Create Step
        let publication = self
            .postgres_connection
            .find_publication_by_name(&self.postgres_config.get_publication_name())
            .await?;

        if publication.is_none() {
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

            log::info!("Source Tables: {source_tables:?}");

            log::info!("Create Publication");
            self.postgres_connection
                .create_publication(&self.postgres_config.get_publication_name(), &source_tables)
                .await?;
        } else {
            log::info!(
                "Publication {} already exists, skipping creation.",
                self.postgres_config.get_publication_name()
            );
        }

        // 2. Publication Tables Add Step
        let publication_tables = self
            .postgres_connection
            .get_publication_tables(&self.postgres_config.get_publication_name())
            .await?;

        for table in &self.postgres_config.tables {
            let table_name = format!("{}.{}", table.schema_name, table.table_name);

            if !publication_tables
                .iter()
                .any(|t| t.table_name == table.table_name && t.schema_name == table.schema_name)
            {
                log::info!("Adding table {table_name} to publication");
                self.postgres_connection
                    .add_table_to_publication(
                        &self.postgres_config.get_publication_name(),
                        &[table_name],
                    )
                    .await?;
            }
        }

        // 3. Replication Slot Create Step
        log::info!("Create Replication Slot");
        let replication_slot = self
            .postgres_connection
            .find_replication_slot_by_name(&self.postgres_config.get_replication_slot_name())
            .await?;

        if replication_slot.is_none() {
            self.postgres_connection
                .create_replication_slot(&self.postgres_config.get_replication_slot_name())
                .await?;
        }

        Ok(())
    }

    async fn setup_table(&mut self) -> Result<(), Errors> {
        log::info!("Setting up table in ClickHouse...");

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
                log::info!("Creating ClickHouse table for {}", table.table_name);
                let create_table_query = self.generate_create_table_query(
                    &self.clickhouse_config.connection.database,
                    &table.table_name,
                    &postgres_columns,
                );

                self.clickhouse_connection
                    .execute_query(&create_table_query)
                    .await?;
            }

            let relation_id = self
                .postgres_connection
                .get_relation_id_by_table_name(&table.schema_name, &table.table_name)
                .await?;

            let clickhouse_columns = self
                .clickhouse_connection
                .list_columns_by_tablename(
                    &self.clickhouse_config.connection.database,
                    &table.table_name,
                )
                .await?;

            self.context.set_table(
                table.schema_name.as_str(),
                table.table_name.as_str(),
                postgres_columns.clone(),
                clickhouse_columns.clone(),
            );
            self.context.table_relation_map.insert(
                relation_id as u32,
                (table.schema_name.clone(), table.table_name.clone()),
            );
        }

        Ok(())
    }
}

impl PostgresPipe {
    async fn first_sync(&self) {
        log::info!("Starting initial sync...");

        for table in &self.postgres_config.tables {
            if table.skip_copy {
                log::info!(
                    "Skipping initial sync for {}.{} as skip_copy is set to true",
                    table.schema_name,
                    table.table_name
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
                log::info!(
                    "Table {} already exists in ClickHouse, skipping initial sync.",
                    table.table_name
                );
                continue;
            }

            let rows = self
                .postgres_connection
                .copy_table_to_stdout(&table.schema_name, &table.table_name)
                .await
                .expect("Failed to copy table data from Postgres");

            let schema_name = &table.schema_name;
            let table_name = &table.table_name;
            let source_table_info = self
                .context
                .tables_map
                .get(&format!("{schema_name}.{table_name}"))
                .expect("Table info not found in context");

            for chunk in rows.chunks(100000) {
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
        }
    }
}

impl PostgresPipe {
    async fn sync_loop(&self) {
        let publication_name = self.postgres_config.get_publication_name();
        let replication_slot_name = self.postgres_config.get_replication_slot_name();

        loop {
            // 1. Peek new rows
            let peek_result = self
                .postgres_connection
                .peek_wal_changes(&publication_name, &replication_slot_name, 65536)
                .await;

            let peek_result = match peek_result {
                Ok(peek) => peek,
                Err(e) => {
                    // 1.1. Handle peek error. wait and retry
                    log::error!("Error peeking WAL changes: {e:?}");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    continue;
                }
            };

            // parse peeked rows

            if peek_result.is_empty() {
                log::info!("No new changes found, waiting for next iteration...");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }

            pub struct Count {
                pub insert_count: usize,
                pub update_count: usize,
                pub delete_count: usize,
            }
            let mut table_log_map = HashMap::new();

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
                        let source_table_info = self
                            .context
                            .tables_map
                            .get(&format!("{schema_name}.{table_name}"))
                            .expect("Table info not found in context");

                        let insert_query = self.generate_insert_query(
                            &self.clickhouse_config,
                            &source_table_info.clickhouse_columns,
                            &source_table_info.postgres_columns,
                            table_name,
                            &[PostgresCopyRow {
                                columns: parsed_row.payload,
                            }],
                        );

                        if let Err(error) = self
                            .clickhouse_connection
                            .execute_query(&insert_query)
                            .await
                        {
                            log::error!(
                                "Failed to execute insert query for {schema_name}.{table_name}: {error}"
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }

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
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
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

            // Advance the exporter
            if !peek_result.is_empty() {
                let advance_key = &peek_result.last().unwrap().lsn;

                if let Err(e) = self
                    .postgres_connection
                    .advance_replication_slot(&replication_slot_name, advance_key)
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
        }
    }
}

impl IntoClickhouse for PostgresPipe {}

pub async fn run_postgres_pipe(config: &Configuraion) {
    let mut pipe = PostgresPipe::new(
        config
            .source
            .postgres
            .clone()
            .expect("Postgres config is required"),
        config
            .target
            .clickhouse
            .clone()
            .expect("Clickhouse config is required"),
    )
    .await;

    if let Err(error) = pipe.ping().await {
        log::error!("Failed to ping Postgres exporter: {error:?}");
        return;
    }

    tokio::select! {
        _ = pipe.run_pipe() => {
            log::info!("Postgres pipe running.");
        }
    }
}
