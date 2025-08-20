use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Configuraion {
    pub source: Source,
    pub target: Target,

    #[serde(default = "default::sleep_millis_when_peek_failed")]
    pub sleep_millis_when_peek_failed: u64,
    #[serde(default = "default::sleep_millis_when_peek_is_empty")]
    pub sleep_millis_when_peek_is_empty: u64,
    #[serde(default = "default::sleep_millis_when_write_failed")]
    pub sleep_millis_when_write_failed: u64,
    #[serde(default = "default::sleep_millis_after_sync_iteration")]
    pub sleep_millis_after_sync_iteration: u64,
    #[serde(default = "default::sleep_millis_after_sync_write")]
    pub sleep_millis_after_sync_write: u64,
    #[serde(default = "default::peek_changes_limit")]
    pub peek_changes_limit: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Source {
    pub source_type: SourceType,
    pub postgres: Option<PostgresConfig>,
    pub mongodb: Option<MongoDBConfig>,
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
    #[serde(rename = "mongodb")]
    MongoDB,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresConfig {
    pub connection: PostgresConnectionConfig,
    pub tables: Vec<PostgresSource>,
    #[serde(default = "default::postgres::publication_name")]
    pub publication_name: String,
    #[serde(default = "default::postgres::replication_slot_name")]
    pub replication_slot_name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MongoDBConfig {
    pub connection: MongoDBConnectionConfig,
    pub collections: Vec<MongoDBSource>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MongoDBConnectionConfig {
    pub host: String,
    pub username: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MongoDBSource {
    pub collection_name: String,
    #[serde(default)]
    pub skip_copy: bool,
    #[serde(default)]
    pub mask_fields: Vec<String>,
}

pub mod default {
    pub mod postgres {
        pub const PUBLICATION_NAME: &str = "clockpipe_publication";
        pub fn publication_name() -> String {
            PUBLICATION_NAME.to_string()
        }

        pub const REPLICATION_SLOT_NAME: &str = "clockpipe_replication_slot";
        pub fn replication_slot_name() -> String {
            REPLICATION_SLOT_NAME.to_string()
        }
    }

    pub mod clickhouse {
        pub const MIN_AGE_TO_FORCE_MERGE_SECONDS: u64 = 60;
        pub fn min_age_to_force_merge_seconds() -> u64 {
            MIN_AGE_TO_FORCE_MERGE_SECONDS
        }

        pub const INDEX_GRANULARITY: u64 = 8192;
        pub fn index_granularity() -> u64 {
            INDEX_GRANULARITY
        }
    }

    pub const PEEK_CHANGES_LIMIT: i64 = 65536;
    pub fn peek_changes_limit() -> i64 {
        PEEK_CHANGES_LIMIT
    }

    pub const SLEEP_MILLIS_WHEN_PEEK_FAILED: u64 = 5000;
    pub fn sleep_millis_when_peek_failed() -> u64 {
        SLEEP_MILLIS_WHEN_PEEK_FAILED
    }

    pub const SLEEP_MILLIS_WHEN_PEEK_IS_EMPTY: u64 = 5000;
    pub fn sleep_millis_when_peek_is_empty() -> u64 {
        SLEEP_MILLIS_WHEN_PEEK_IS_EMPTY
    }

    pub const SLEEP_MILLIS_WHEN_WRITE_FAILED: u64 = 5000;
    pub fn sleep_millis_when_write_failed() -> u64 {
        SLEEP_MILLIS_WHEN_WRITE_FAILED
    }

    pub const SLEEP_MILLIS_AFTER_SYNC_ITERATION: u64 = 100;
    pub fn sleep_millis_after_sync_iteration() -> u64 {
        SLEEP_MILLIS_AFTER_SYNC_ITERATION
    }

    pub const SLEEP_MILLIS_AFTER_SYNC_WRITE: u64 = 100;
    pub fn sleep_millis_after_sync_write() -> u64 {
        SLEEP_MILLIS_AFTER_SYNC_WRITE
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
    #[serde(default)]
    pub mask_columns: Vec<String>,
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
