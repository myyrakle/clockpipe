pub mod postgres;
pub use postgres::*;

use crate::errors::Errors;

#[async_trait::async_trait]
pub trait IPipe {
    async fn ping(&self) -> Result<(), Errors>;
    async fn run_pipe(&mut self);
}
