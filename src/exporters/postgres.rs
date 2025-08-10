use crate::{
    adapter,
    errors::Errors,
    interface::{IExporter, PeekResult},
};

#[derive(Clone)]
pub struct PostgresExporter {
    pub postgres_config: crate::config::PostgresConfig,
    pub clickhouse_config: crate::config::ClickHouseConfig,
    postgres_connection: adapter::postgres::PostgresConnection,
    clickhouse_connection: adapter::clickhouse::ClickhouseConnection,
}

impl PostgresExporter {
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

        PostgresExporter {
            postgres_config,
            clickhouse_config,
            postgres_connection,
            clickhouse_connection,
        }
    }
}

#[async_trait::async_trait]
impl IExporter for PostgresExporter {
    async fn ping(&self) -> Result<(), Errors> {
        self.postgres_connection
            .ping()
            .await
            .map_err(|e| Errors::DatabasePingError(format!("Postgres ping failed: {}", e)))?;

        self.clickhouse_connection
            .ping()
            .await
            .map_err(|e| Errors::DatabasePingError(format!("ClickHouse ping failed: {}", e)))?;

        println!("Postgres and ClickHouse connections are healthy.");

        Ok(())
    }

    async fn setup(&self) -> Result<(), Errors> {
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

            println!("Source Tables: {:?}", source_tables);

            println!("Create Publication");
            self.postgres_connection
                .create_publication(&self.postgres_config.get_publication_name(), &source_tables)
                .await?;
        } else {
            println!(
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
                println!("Adding table {} to publication", table_name);
                self.postgres_connection
                    .add_table_to_publication(
                        &self.postgres_config.get_publication_name(),
                        &[table_name],
                    )
                    .await?;
            }
        }

        // 3. Replication Slot Create Step
        println!("Create Replication Slot");
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

    async fn peek(&self) -> Result<PeekResult, Errors> {
        unimplemented!("Postgres peek not implemented yet");
    }

    async fn advance(&self, _key: &str) -> Result<(), Errors> {
        unimplemented!("Postgres advance not implemented yet");
    }
}
