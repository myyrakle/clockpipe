pub mod pipe;
use clap::Parser;
pub mod adapter;
mod command;
pub mod config;
pub mod errors;
pub mod exporters;
pub mod interface;

#[tokio::main]
async fn main() {
    let args = command::Command::parse();

    match args.action {
        command::SubCommand::Run(command) => {
            println!("config-file: {}", command.value.config_file);

            pipe::run_postgres_pipe(&command.value).await;
        }
    }
}
