use crate::errors;

#[derive(Clone)]
pub struct ClickhouseConnection {
    client: clickhouse::Client,
}

impl ClickhouseConnection {
    pub fn new(config: &crate::config::ClickHouseConnectionConfig) -> Self {
        let client = clickhouse::Client::default()
            .with_url(format!("http://{}:{}", config.host, config.port))
            .with_user(config.username.clone())
            .with_password(config.password.clone())
            .with_database(config.database.clone());

        ClickhouseConnection { client }
    }

    pub async fn ping(&self) -> errors::Result<()> {
        self.client
            .query("SELECT 1")
            .fetch_one::<u8>()
            .await
            .map_err(|e| {
                crate::errors::Errors::DatabasePingError(format!(
                    "Failed to ping ClickHouse: {}",
                    e
                ))
            })?;

        Ok(())
    }
}
