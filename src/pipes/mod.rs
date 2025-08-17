pub mod postgres;
pub use postgres::*;

use crate::errors::Errors;

#[async_trait::async_trait]
pub trait IPipe {
    async fn ping(&self) -> Result<(), Errors>;

    async fn run_pipe(&mut self) {
        self.initialize().await;

        self.first_sync().await;
        self.sync_loop().await;
    }

    async fn initialize(&mut self);
    async fn first_sync(&self);
    async fn sync_loop(&self);
}

#[derive(Debug, Clone, Default)]
pub struct WriteCounter {
    pub insert_count: usize,
    pub update_count: usize,
    pub delete_count: usize,
}
