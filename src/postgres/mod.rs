use crate::{
    errors::Errors,
    interface::{IExporter, PeekResult},
};

#[derive(Debug, Clone)]
pub struct PostgresExporter {
    pub config: Option<crate::config::PostgresConfig>,
}

#[async_trait::async_trait]
impl IExporter for PostgresExporter {
    async fn ping(&self) -> Result<(), Errors> {
        // Implement ping logic for Postgres
        Ok(())
    }

    async fn peek(&self) -> Result<PeekResult, Errors> {
        unimplemented!("Postgres peek not implemented yet");
    }

    async fn advance(&self, _key: &str) -> Result<(), Errors> {
        unimplemented!("Postgres advance not implemented yet");
    }
}
