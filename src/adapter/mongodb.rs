use std::{path::PathBuf, time::Duration};

use futures::StreamExt;
use mongodb::{
    Client,
    bson::{Document, doc},
    change_stream::event::{OperationType, ResumeToken},
    options::{ServerApi, ServerApiVersion},
};
use tokio::sync::oneshot;

use crate::{config::MongoDBConfig, errors};

#[derive(Debug, Clone)]
pub struct MongoDBConnection {
    client: Client,
    resume_token_storage: ResumeTokenStorage,
}

#[derive(Debug, Clone)]
pub enum ResumeTokenStorage {
    File(PathBuf),
}

impl MongoDBConnection {
    pub async fn new(config: &MongoDBConfig) -> errors::Result<Self> {
        println!("{:?}", config);

        let connection_config = &config.connection;

        let connection_string = format!(
            "mongodb+srv://{}:{}@{}/{}",
            connection_config.username,
            connection_config.password,
            connection_config.host,
            connection_config.database
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

        let resume_token_storage =
            ResumeTokenStorage::File(PathBuf::from(config.resume_token_path.clone()));

        Ok(Self {
            client,
            resume_token_storage,
        })
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

    pub async fn peek_changes(
        &self,
        database_name: &str,
        limit: i64,
        timeout_ms: u64,
    ) -> errors::Result<PeekMongoChangesResult> {
        let database = self.client.database(database_name);

        let mut watch = database.watch();
        watch = watch.batch_size(5).max_await_time(Duration::from_secs(10));

        let mut resume_token = if let Some(resume_token) = self.load_resume_token()? {
            log::debug!("Resume token found, resuming from it");
            Some(resume_token)
        } else {
            log::info!("No resume token found, starting from the beginning");
            None
        };

        if let Some(token) = &resume_token {
            watch = watch.start_after(token.clone());
        }

        let mut watch = watch.await.expect("Failed to create change stream");

        if resume_token.is_none() {
            resume_token = watch.resume_token();
        }

        let mut resume_token = resume_token.expect("Failed to get initial resume token");

        let mut changes = Vec::new();
        changes.reserve(limit as usize);

        let (timeout_sender, mut timeout_receiver) = oneshot::channel();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
            let _ = timeout_sender.send(());
        });

        loop {
            tokio::select! {
                _ = (&mut timeout_receiver) => {
                    println!("Timeout reached");
                    break;
                }
                Some(event) = watch.next() => {
                    let event = event.map_err(|e| {
                        errors::Errors::PeekWalChangesFailed(format!("Failed to get next event: {e}"))
                    })?;

                    println!("Watching changes in the database...");

                    let operation_type = event.operation_type;
                    let document_key = event.document_key;
                    let full_document = event.full_document;

                    println!("Operation Type: {:?}", operation_type);
                    println!("Document Key: {:?}", document_key);
                    println!("Full Document: {:?}", full_document);

                    changes.push(PeekMongoChange {
                        operation_type,
                        document_key,
                        full_document,
                    });

                    resume_token = watch.resume_token().expect("Failed to get resume token");

                    if changes.len() as i64 >= limit {
                        break;
                    }
                }
            }
        }

        Ok(PeekMongoChangesResult {
            changes,
            resume_token,
        })
    }

    pub fn store_resume_token(&self, token: &ResumeToken) -> errors::Result<()> {
        match &self.resume_token_storage {
            ResumeTokenStorage::File(path) => {
                let json = serde_json::to_string(token).map_err(|e| {
                    errors::Errors::DatabaseConnectionError(format!(
                        "Failed to serialize resume token: {e}"
                    ))
                })?;

                std::fs::write(path, json).map_err(|e| {
                    errors::Errors::DatabaseConnectionError(format!(
                        "Failed to write resume token to file: {e}"
                    ))
                })?;

                Ok(())
            }
        }
    }

    fn load_resume_token(&self) -> errors::Result<Option<ResumeToken>> {
        match &self.resume_token_storage {
            ResumeTokenStorage::File(path) => {
                // if not exists, return None
                if !path.exists() {
                    return Ok(None);
                }

                let json = std::fs::read_to_string(path).or_else(|e| {
                    Err(errors::Errors::DatabaseConnectionError(format!(
                        "Failed to read resume token file: {e}"
                    )))
                })?;

                let token: ResumeToken = serde_json::from_str(&json).map_err(|e| {
                    errors::Errors::DatabaseConnectionError(format!(
                        "Failed to parse resume token: {e}"
                    ))
                })?;

                Ok(Some(token))
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeekMongoChange {
    pub operation_type: OperationType,
    pub document_key: Option<Document>,
    pub full_document: Option<Document>,
}

#[derive(Debug, Clone)]
pub struct PeekMongoChangesResult {
    pub changes: Vec<PeekMongoChange>,
    pub resume_token: ResumeToken,
}
