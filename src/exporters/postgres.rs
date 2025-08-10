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
        println!("Create Publication");
        self.postgres_connection
            .create_publication(&self.postgres_config.get_publication_name(), &[])
            .await?;

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
