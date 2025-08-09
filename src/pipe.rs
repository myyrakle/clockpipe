use crate::{command, interface::IExporter, postgres};

pub async fn run_postgres_pipe(config_options: &command::run::ConfigOptions) {
    let config = config_options
        .read_config_from_file()
        .expect("Failed to read configuration");

    let postgres_pipe = new_pipe(postgres::PostgresExporter {
        config: config.postgres.clone(),
    })
    .await;

    tokio::select! {
        _ = postgres_pipe.run_pipe() => {
            println!("Postgres exporter running.");
        }
    }
}

#[derive(Debug)]
pub struct Pipe<T: IExporter> {
    pub exporter: T,
}

pub async fn new_pipe<T: IExporter>(exporter: T) -> Pipe<T> {
    Pipe { exporter }
}

impl<T: IExporter> Pipe<T> {
    async fn run_pipe(&self) {
        self.initialize().await;
        self.sync().await;
    }

    async fn initialize(&self) {
        println!("Initializing Postgres exporter...");

        // create publication if not exists

        // create replication if not exists

        // create table if not exists & copy data from the table
    }

    async fn sync(&self) {
        loop {
            // Peek new rows
            let peek_result = self.exporter.peek().await;

            let peek_result = match peek_result {
                Ok(peek) => peek,
                Err(e) => {
                    eprintln!("Error peeking: {:?}", e);
                    continue;
                }
            };

            // Handle peek result
            // ...

            // Advance the exporter
            if let Err(e) = self.exporter.advance(&peek_result.advance_key).await {
                eprintln!("Error advancing exporter: {:?}", e);
                continue;
            }
        }
    }
}
