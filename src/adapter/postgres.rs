use crate::{config::PostgresConnectionConfig, errors};

#[derive(Debug)]
pub struct PostgresConnection {
    pool: sqlx::Pool<sqlx::Postgres>,
}

impl PostgresConnection {
    pub async fn new(config: PostgresConnectionConfig) -> errors::Result<Self> {
        let connection_string = format!(
            "host={} port={} user={} password={}",
            config.host, config.port, config.username, config.password
        );

        let result = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(&connection_string)
            .await;

        match result {
            Ok(pool) => {
                println!("Successfully connected to Postgres database");

                Ok(PostgresConnection { pool })
            }
            Err(e) => {
                return Err(errors::Errors::DatabaseConnectionError(format!(
                    "Failed to connect to Postgres database: {}",
                    e
                )));
            }
        }
    }

    pub async fn ping(&self) -> errors::Result<()> {
        let result = sqlx::query("SELECT 1").execute(&self.pool).await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(errors::Errors::DatabasePingError(format!(
                "Failed to ping Postgres database: {}",
                e
            ))),
        }
    }
}
