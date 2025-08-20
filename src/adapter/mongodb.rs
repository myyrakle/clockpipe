use mongodb::{
    Client,
    bson::doc,
    options::{ServerApi, ServerApiVersion},
};

use crate::{config::MongoDBConnectionConfig, errors};

#[derive(Debug, Clone)]
pub struct MongoDBConnection {
    client: Client,
}

impl MongoDBConnection {
    pub async fn new(config: &MongoDBConnectionConfig) -> errors::Result<Self> {
        println!("{:?}", config);

        let connection_string = format!(
            "mongodb+srv://{}:{}@{}/{}",
            config.username, config.password, config.host, config.database
        );

        let mut client_options = mongodb::options::ClientOptions::parse(&connection_string)
            .await
            .map_err(|e| {
                errors::Errors::DatabaseConnectionError(format!(
                    "Failed to parse MongoDB connection string: {e}"
                ))
            })?;

        let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
        client_options.server_api = Some(server_api);

        let client = Client::with_options(client_options).map_err(|e| {
            errors::Errors::DatabaseConnectionError(format!("Failed to create MongoDB client: {e}"))
        })?;

        Ok(Self { client })
    }

    pub async fn ping(&self) -> errors::Result<()> {
        let result = self
            .client
            .database("admin")
            .run_command(doc! { "ping": 1 })
            .await;

        match result {
            Ok(_) => Ok(()),
            Err(e) => Err(errors::Errors::DatabasePingError(format!(
                "Failed to ping MongoDB database: {e}"
            ))),
        }
    }
}
