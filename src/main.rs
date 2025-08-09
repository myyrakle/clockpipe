pub mod pipe;
use clap::Parser;
mod command;
pub mod errors;
pub mod interface;
pub mod postgres;

#[tokio::main]
async fn main() {
    let args = command::Command::parse();

    match args.action {
        command::SubCommand::Run(command) => {
            println!("{}", command.value.config_file);
            pipe::run_postgres_pipe().await;
        }
    }
}
