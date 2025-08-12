use crate::errors::Errors;

#[async_trait::async_trait]
pub trait IPipe {
    async fn ping(&self) -> Result<(), Errors>;
    async fn run_pipe(&mut self);
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
