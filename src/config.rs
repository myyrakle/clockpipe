use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Configuraion {
    pub source_type: SourceType,
    pub postgres: Option<PostgresConfig>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum SourceType {
    #[serde(rename = "postgres")]
    Postgres,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresConfig {
    pub connection: PostgresConnection,
    pub tables: Vec<PostgresSource>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresConnection {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PostgresSource {
    pub database_name: String,
    pub table_name: String,
}
