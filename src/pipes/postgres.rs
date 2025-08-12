use serde::de;

use crate::{
    adapter::{
        self,
        clickhouse::ClickhouseColumn,
        postgres::{
            PostgresColumn, PostgresCopyRow,
            mapper::generate_clickhouse_create_table_query,
            pgoutput::{MessageType, parse_pg_output},
        },
    },
    command,
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
            format!("{}.{}", schema_name, table_name),
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
            .map_err(|e| Errors::DatabasePingError(format!("Postgres ping failed: {}", e)))?;

        self.clickhouse_connection
            .ping()
            .await
            .map_err(|e| Errors::DatabasePingError(format!("ClickHouse ping failed: {}", e)))?;

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

            log::info!("Source Tables: {:?}", source_tables);

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
                log::info!("Adding table {} to publication", table_name);
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
            let relation_id = self
                .postgres_connection
                .get_relation_id_by_table_name(&table.schema_name, &table.table_name)
                .await?;

            let postgres_columns = self
                .postgres_connection
                .list_columns_by_tablename(&table.schema_name, &table.table_name)
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

            if !clickhouse_columns.is_empty() {
                // TODO: 컬럼이 추가된 경우에 대한 대응

                log::info!(
                    "{}.{} Table is Already exists in ClickHouse, skipping creation.",
                    table.schema_name,
                    table.table_name,
                );

                continue;
            }

            log::info!("Creating ClickHouse table for {}", table.table_name);
            let create_table_query = generate_clickhouse_create_table_query(
                &self.clickhouse_config.connection.database,
                &table.table_name,
                &postgres_columns,
            );

            self.clickhouse_connection
                .execute_query(&create_table_query)
                .await?;
        }

        Ok(())
    }
}

impl PostgresPipe {
    async fn first_sync(&self) {
        log::info!("Starting initial sync...");

        for table in &self.postgres_config.tables {
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

            for chunk in rows.chunks(100000) {
                let insert_query =
                    self.generate_insert_query(&table.schema_name, &table.table_name, chunk);

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
                    log::error!("Error peeking WAL changes: {:?}", e);
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
                        let insert_query = self.generate_insert_query(
                            &schema_name,
                            &table_name,
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
                                "Failed to execute insert query for {}.{}: {}",
                                schema_name,
                                table_name,
                                error
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
                    }
                    MessageType::Delete => {
                        let delete_query = self.generate_delete_query(
                            &schema_name,
                            &table_name,
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
                                "Failed to execute delete query for {}.{}: {}",
                                schema_name,
                                table_name,
                                error
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                            continue;
                        }
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
                    log::error!("Error advancing exporter: {:?}", e);
                    continue;
                }
            }
        }
    }
}

impl PostgresPipe {
    fn generate_insert_query(
        &self,
        schema_name: &str,
        table_name: &str,
        rows: &[PostgresCopyRow],
    ) -> String {
        if rows.is_empty() {
            return String::new();
        }

        let mut insert_query = format!(
            "INSERT INTO {}.{table_name} ",
            self.clickhouse_config.connection.database
        );

        let table_info = self
            .context
            .tables_map
            .get(&format!("{}.{}", schema_name, table_name))
            .expect("Table info not found in context");

        let mut columns = vec![];
        let mut column_names = vec![];

        for clickhouse_column in &table_info.clickhouse_columns {
            let Some(postgres_column) = table_info
                .postgres_columns
                .iter()
                .find(|col| col.column_name == clickhouse_column.column_name)
            else {
                continue;
            };

            columns.push((clickhouse_column.clone(), postgres_column.clone()));
            column_names.push(clickhouse_column.column_name.clone());
        }

        insert_query.push_str(&format!("({}) ", column_names.join(", ")));
        insert_query.push_str("VALUES");

        let mut values = vec![];

        for row in rows {
            let mut value = vec![];

            for (clickhouse_column, _) in columns.iter() {
                let column_index = (clickhouse_column.column_index - 1) as usize;

                let value_column = match row.columns.get(column_index) {
                    Some(raw_value) => clickhouse_column.value(raw_value.to_owned()),
                    _ => clickhouse_column.default_value(),
                };

                value.push(value_column);
            }

            let value = value.join(",");
            values.push(format!("({})", value));
        }

        insert_query.push_str(values.join(", ").as_str());

        insert_query
    }

    fn generate_delete_query(
        &self,
        schema_name: &str,
        table_name: &str,
        row: &PostgresCopyRow,
    ) -> String {
        if row.columns.is_empty() {
            return String::new();
        }

        let mut delete_query = format!(
            "ALTER TABLE {}.{table_name} DELETE WHERE ",
            self.clickhouse_config.connection.database
        );

        let table_info = self
            .context
            .tables_map
            .get(&format!("{}.{}", schema_name, table_name))
            .expect("Table info not found in context");

        let mut conditions = vec![];

        for (index, column) in table_info.clickhouse_columns.iter().enumerate() {
            if !column.is_in_primary_key {
                continue;
            }

            let value = row.columns[index].text_ref_or("");
            conditions.push(format!(
                "{} = '{}'",
                column.column_name,
                value.replace("'", "''")
            ));
        }

        delete_query.push_str(&conditions.join(" AND "));

        delete_query
    }
}

pub async fn run_postgres_pipe(config_options: &command::run::ConfigOptions) {
    let config = config_options
        .read_config_from_file()
        .expect("Failed to read configuration");

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
        log::error!("Failed to ping Postgres exporter: {:?}", error);
        return;
    }

    tokio::select! {
        _ = pipe.run_pipe() => {
            log::info!("Postgres pipe running.");
        }
    }
}
