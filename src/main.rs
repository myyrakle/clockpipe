pub mod pipe;
use std::env;

use clap::Parser;
pub mod adapter;
mod command;
pub mod config;
pub mod errors;
pub mod exporters;
pub mod interface;
use log::info;

#[tokio::main]
async fn main() {
    unsafe {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = command::Command::parse();

    match args.action {
        command::SubCommand::Run(command) => {
            info!("config-file: {}", command.value.config_file);

            pipe::run_postgres_pipe(&command.value).await;
        }
    }
}
