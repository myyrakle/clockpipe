use std::env;

use clap::Parser;
pub mod adapter;
mod command;
pub mod config;
pub mod errors;
pub mod interface;
pub mod pipes;

#[tokio::main]
async fn main() {
    unsafe {
        env::set_var("RUST_LOG", "info");
    }
    env_logger::init();

    let args = command::Command::parse();

    match args.action {
        command::SubCommand::Run(command) => {
            log::info!("config-file: {}", command.value.config_file);

            pipes::run_postgres_pipe(&command.value).await;
        }
    }
}
