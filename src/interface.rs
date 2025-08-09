use crate::errors::Errors;

#[derive(Debug, Clone)]
pub enum SourceType {
    Postgres,
}

#[async_trait::async_trait]
pub trait IExporter {
    async fn peek(&self) -> Result<PeekResult, Errors>;
    async fn advance(&self, key: &str) -> Result<(), Errors>;
}

#[derive(Debug, Clone)]
pub enum RowKey {
    StringKey(String),
    IntegerKey(i64),
}

#[derive(Debug, Clone)]
pub enum EventType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct Row {
    pub event_type: EventType,
    pub key: RowKey,
}

#[derive(Debug, Clone)]
pub struct PeekResult {
    pub rows: Vec<Row>,
    pub advance_key: String,
}
