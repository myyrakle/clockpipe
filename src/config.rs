use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Configuraion {
    pub source: Source,
    pub target: Target,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Source {
    pub source_type: SourceType,
    pub postgres: Option<PostgresConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Target {
    pub target_type: TargetType,
    pub clickhouse: Option<ClickHouseConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum SourceType {
    #[serde(rename = "postgres")]
    Postgres,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresConfig {
    pub connection: PostgresConnectionConfig,
    pub tables: Vec<PostgresSource>,
    pub publication_name: Option<String>,
    pub replication_slot_name: Option<String>,
    pub sleep_millis_when_peek_failed: Option<u64>,
}

pub mod default {
    pub mod postgres {
        pub const PUBLICATION_NAME: &str = "clockpipe_publication";
        pub const REPLICATION_SLOT_NAME: &str = "clockpipe_replication_slot";
        pub const PEEK_CHANGES_LIMIT: i64 = 65536;
        pub const SLEEP_MILLIS_WHEN_PEEK_FAILED: u64 = 5000;
    }
}

impl PostgresConfig {
    pub fn get_publication_name(&self) -> &str {
        self.publication_name
            .as_deref()
            .unwrap_or(default::postgres::PUBLICATION_NAME)
    }

    pub fn get_replication_slot_name(&self) -> &str {
        self.replication_slot_name
            .as_deref()
            .unwrap_or(default::postgres::REPLICATION_SLOT_NAME)
    }

    pub fn get_sleep_millis_when_peek_failed(&self) -> u64 {
        self.sleep_millis_when_peek_failed
            .unwrap_or(default::postgres::SLEEP_MILLIS_WHEN_PEEK_FAILED)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresConnectionConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

impl PostgresConnectionConfig {
    pub fn connection_string(&self) -> String {
        format!(
            "postgres://{}:{}@{}:{}/{}",
            self.username, self.password, self.host, self.port, self.database
        )
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresSource {
    pub schema_name: String,
    pub table_name: String,
    #[serde(default)]
    pub skip_copy: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum TargetType {
    #[serde(rename = "clickhouse")]
    ClickHouse,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClickHouseConfig {
    pub connection: ClickHouseConnectionConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ClickHouseConnectionConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}
