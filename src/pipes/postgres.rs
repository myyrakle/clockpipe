use crate::{
    adapter::{self, postgres::mapper::generate_clickhouse_create_table_query},
    command,
    errors::Errors,
    interface::{IPipe, PeekResult},
};

#[derive(Clone)]
pub struct PostgresPipe {
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

    async fn run_pipe(&self) {
        self.initialize().await;
        self.first_sync().await;
        self.sync_loop().await;
    }
}

impl PostgresPipe {
    async fn initialize(&self) {
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

    async fn setup_table(&self) -> Result<(), Errors> {
        log::info!("Setting up table in ClickHouse...");

        for table in &self.postgres_config.tables {
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

            for _chunk in rows.chunks(100000) {}
        }
    }
}

impl PostgresPipe {
    async fn sync_loop(&self) {
        loop {
            // Peek new rows
            let peek_result = self.peek().await;

            let peek_result = match peek_result {
                Ok(peek) => peek,
                Err(e) => {
                    log::error!("Error peeking: {:?}", e);
                    continue;
                }
            };

            // Handle peek result
            // ...

            // Advance the exporter
            if let Err(e) = self.advance(&peek_result.advance_key).await {
                log::error!("Error advancing exporter: {:?}", e);
                continue;
            }
        }
    }

    async fn peek(&self) -> Result<PeekResult, Errors> {
        unimplemented!("Postgres peek not implemented yet");
    }

    async fn advance(&self, _key: &str) -> Result<(), Errors> {
        unimplemented!("Postgres advance not implemented yet");
    }
}

pub async fn run_postgres_pipe(config_options: &command::run::ConfigOptions) {
    let config = config_options
        .read_config_from_file()
        .expect("Failed to read configuration");

    let pipe = PostgresPipe::new(
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
