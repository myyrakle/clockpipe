use sqlx::mysql::MySqlConnectOptions;

use crate::{config::MySQLConnectionConfig, errors};

pub mod binlog;

#[derive(Debug, Clone)]
pub struct MySQLConnection {
    pool: sqlx::Pool<sqlx::MySql>,
    config: MySQLConnectionConfig,
}

impl MySQLConnection {
    pub async fn new(config: &MySQLConnectionConfig) -> errors::Result<Self> {
        let mut options = MySqlConnectOptions::new()
            .host(&config.host)
            .port(config.port)
            .username(&config.username)
            .database(&config.database);

        if !config.password.is_empty() {
            options = options.password(&config.password);
        }

        let result = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await;

        match result {
            Ok(pool) => {
                log::info!("Successfully connected to MySQL database");

                Ok(MySQLConnection {
                    pool,
                    config: config.clone(),
                })
            }
            Err(e) => Err(errors::Errors::DatabaseConnectionError(format!(
                "Failed to connect to MySQL database: {e}"
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
