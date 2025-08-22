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
                    log::info!("Start Postgres pipe");

                    pipes::run_postgres_pipe(config).await;
                }
                config::SourceType::MongoDB => {
                    log::info!("Start MongoDB pipe");

                    // let mongodb_connection =
                    //     adapter::mongodb::MongoDBConnection::new(&config.source.mongodb.unwrap())
                    //         .await
                    //         .expect("Failed to create MongoDB connection");

                    // let result = mongodb_connection
                    //     .copy_collection_data("cdctest", "log", 10000)
                    //     .await
                    //     .unwrap();
                    // println!("MongoDB copy result: {:?}", result);

                    // let changes = mongodb_connection
                    //     .peek_changes("cdctest", 10, 10000)
                    //     .await
                    //     .unwrap();
                    // println!("MongoDB changes: {:?}", changes);

                    // mongodb_connection
                    //     .store_resume_token(&changes.resume_token)
                    //     .expect("Failed to store resume token");

                    unimplemented!("MongoDB pipe is not implemented yet");
                }
            }
        }
    }
}
