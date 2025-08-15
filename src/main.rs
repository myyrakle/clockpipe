use std::env;

use clap::Parser;
pub mod adapter;
mod command;
pub mod config;
pub mod errors;
pub mod pipes;

fn setup_logging() {
    unsafe {
        if env::var("RUST_LOG").is_err() {
            env::set_var("RUST_LOG", "info");
        }
    }
    env_logger::init();
}

#[tokio::main]
async fn main() {
    setup_logging();

    let args = command::Command::parse();

    match args.action {
        command::SubCommand::Run(command) => {
            log::info!("config-file: {}", command.value.config_file);

            let config = command
                .value
                .read_config_from_file()
                .expect("Failed to read configuration");

            match config.source.source_type {
                config::SourceType::Postgres => {
                    pipes::run_postgres_pipe(&config).await;
                }
            }
        }
    }
}
