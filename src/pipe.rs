use crate::{command, exporters, interface::IExporter};
use log::{error, info};

pub async fn run_postgres_pipe(config_options: &command::run::ConfigOptions) {
    let config = config_options
        .read_config_from_file()
        .expect("Failed to read configuration");

    let exporter = exporters::PostgresExporter::new(
        config
            .source
            .postgres
            .clone()
            .expect("Postgres config is required"),
        config
            .target
            .clickhouse
            .clone()
            .expect("Clickhouse config is required"),
    )
    .await;

    let postgres_pipe = new_pipe(exporter).await;

    if let Err(error) = postgres_pipe.exporter.ping().await {
        error!("Failed to ping Postgres exporter: {:?}", error);
        return;
    }

    tokio::select! {
        _ = postgres_pipe.run_pipe() => {
            info!("Postgres exporter running.");
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
        self.exporter.initialize().await;
        self.exporter.sync().await;
    }
}
