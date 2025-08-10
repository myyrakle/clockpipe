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
                .map(|table| match &table.database_name {
                    Some(db_name) => format!("{}.{}", db_name, table.table_name),
                    None => table.table_name.clone(),
                })
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

        println!("Create Replication Slot");
        self.postgres_connection
            .create_replication_slot(&self.postgres_config.get_replication_slot_name())
            .await?;

        Ok(())
    }

    async fn peek(&self) -> Result<PeekResult, Errors> {
        unimplemented!("Postgres peek not implemented yet");
    }

    async fn advance(&self, _key: &str) -> Result<(), Errors> {
        unimplemented!("Postgres advance not implemented yet");
    }
}
