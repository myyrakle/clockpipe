use std::{path::PathBuf, time::Duration};

use base64::Engine;
use futures::{StreamExt, TryStreamExt};
use mongodb::{
    Client,
    bson::{Bson, Document, doc, spec::ElementType},
    change_stream::event::{OperationType, ResumeToken},
    options::{CursorType, FindOptions, ServerApi, ServerApiVersion},
};
use tokio::sync::oneshot;

use crate::{
    adapter::{
        IntoClickhouseColumn, IntoClickhouseRow, IntoClickhouseValue, clickhouse::ClickhouseType,
    },
    config::MongoDBConfig,
    errors,
};

#[derive(Debug, Clone)]
pub struct MongoDBConnection {
    client: Client,
    resume_token_storage: ResumeTokenStorage,
    copy_batch_size: u32,
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
            copy_batch_size: config.copy_batch_size,
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

    // Copies data from a MongoDB collection to a vector of documents.
    // The `batch_size` parameter specifies how many documents to fetch at once.
    // Returns a vector of documents.
    // If the collection does not exist, it returns an empty vector.
    pub async fn copy_collection(
        &self,
        database_name: &str,
        collection_name: &str,
    ) -> errors::Result<Vec<MongoDBCopyRow>> {
        let database = self.client.database(database_name);
        let collection = database.collection::<Document>(collection_name);

        let find_options = FindOptions::builder()
            .batch_size(self.copy_batch_size) // 한 번에 가져올 문서 수
            .cursor_type(CursorType::NonTailable)
            .build();

        let mut cursor = collection
            .find(doc! {})
            .with_options(find_options)
            .await
            .map_err(|e| {
                errors::Errors::DatabaseConnectionError(format!("Failed to create cursor: {e}"))
            })?;

        let mut documents = Vec::new();

        while let Some(doc) = cursor.try_next().await.map_err(|e| {
            errors::Errors::DatabaseConnectionError(format!("Failed to fetch document: {e}"))
        })? {
            documents.push(doc);
        }

        Ok(documents
            .into_iter()
            .map(|doc| MongoDBCopyRow {
                columns: doc
                    .into_iter()
                    .map(|(k, v)| MongoDBColumn {
                        column_name: k,
                        bson_value: v,
                    })
                    .collect(),
            })
            .collect())
    }

    // Peeks changes in the MongoDB database.
    // Returns a vector of changes and a resume token.
    // The resume token can be used to continue watching changes from the last point.
    // The `limit` parameter specifies the maximum number of changes to return.
    // The `timeout_ms` parameter specifies the maximum time to wait for changes.
    // If no changes are available within the timeout, an empty vector is returned.
    pub async fn peek_changes(
        &self,
        database_name: &str,
        collection_names: &[&str],
        limit: u64,
        timeout_ms: u64,
    ) -> errors::Result<PeekMongoChangesResult> {
        let database = self.client.database(database_name);

        let mut watch = database.watch();

        watch = watch.full_document(mongodb::options::FullDocumentType::UpdateLookup);

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

        let mut watch = watch.await.map_err(|e| {
            errors::Errors::PeekChangesFailed(format!("Failed to start watching changes: {e}"))
        })?;

        // If no resume token is available, we will try to get it from the watch.
        if resume_token.is_none() {
            resume_token = watch.resume_token();
        }

        let mut resume_token = resume_token.ok_or_else(|| {
            errors::Errors::PeekChangesFailed("No resume token available".to_string())
        })?;

        let mut changes = Vec::with_capacity(limit as usize);

        let (timeout_sender, mut timeout_receiver) = oneshot::channel();

        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
            let _ = timeout_sender.send(());
        });

        loop {
            tokio::select! {
                _ = (&mut timeout_receiver) => {
                    log::debug!("Timeout reached");
                    break;
                }
                Some(event) = watch.next() => {
                    let event = event.map_err(|e| {
                        errors::Errors::PeekChangesFailed(format!("Failed to get next event: {e}"))
                    })?;

                    let operation_type = event.operation_type;
                    let document_key = event.document_key;
                    let full_document = event.full_document;

                    let collection_name = event.ns.map(|ns| ns.coll).flatten().unwrap_or_default();
                    if collection_names.iter().any(|&name| name == collection_name) {
                        changes.push(PeekMongoChange {
                            operation_type,
                            document_key,
                            full_document,
                            collection_name,
                        });
                    }

                    resume_token = watch.resume_token().ok_or_else(|| {
                        errors::Errors::PeekChangesFailed("Failed to get resume token".to_string())
                    })?;

                    if changes.len()  >= limit as usize {
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

                let json = std::fs::read_to_string(path).map_err(|e| {
                    errors::Errors::DatabaseConnectionError(format!(
                        "Failed to read resume token file: {e}"
                    ))
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
    pub collection_name: String,
}

impl PeekMongoChange {
    pub fn to_copy_row(&self) -> Option<MongoDBCopyRow> {
        match self.operation_type {
            OperationType::Delete => {
                if let Some(doc) = &self.document_key {
                    return Some(MongoDBCopyRow {
                        columns: doc
                            .iter()
                            .map(|(k, v)| MongoDBColumn {
                                column_name: k.clone(),
                                bson_value: v.clone(),
                            })
                            .collect(),
                    });
                } else {
                    return None;
                }
            }
            OperationType::Insert | OperationType::Update => {
                self.full_document
                    .as_ref()
                    .map(|doc: &Document| MongoDBCopyRow {
                        columns: doc
                            .iter()
                            .map(|(k, v)| MongoDBColumn {
                                column_name: k.clone(),
                                bson_value: v.clone(),
                            })
                            .collect(),
                    })
            }
            _ => {
                return None;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct PeekMongoChangesResult {
    pub changes: Vec<PeekMongoChange>,
    pub resume_token: ResumeToken,
}

#[derive(Debug, Clone, Default)]
pub struct MongoDBColumn {
    pub column_name: String,
    pub bson_value: Bson,
}

impl IntoClickhouseValue for MongoDBColumn {
    fn to_integer(self) -> String {
        match self.bson_value {
            Bson::Int32(v) => v.to_string(),
            Bson::Int64(v) => v.to_string(),
            Bson::Decimal128(v) => v.to_string(),
            _ => "0".to_string(),
        }
    }

    fn to_real(self) -> String {
        match self.bson_value {
            Bson::Double(v) => v.to_string(),
            Bson::Decimal128(v) => v.to_string(),
            _ => "0.0".to_string(),
        }
    }

    fn to_bool(self) -> String {
        self.bson_value
            .as_bool()
            .map_or("false".to_string(), |v| v.to_string())
    }

    fn to_string(self) -> String {
        match self.bson_value {
            Bson::ObjectId(oid) => format!("'{}'", oid.to_hex()),
            Bson::DateTime(dt) => format!(
                "'{}'",
                chrono::DateTime::<chrono::Utc>::from_timestamp_millis(dt.timestamp_millis())
                    .unwrap_or_default()
                    .format("%Y-%m-%d %H:%M:%S")
            ),
            Bson::Timestamp(ts) => format!(
                "'{}'",
                chrono::DateTime::from_timestamp(ts.time as i64, 0)
                    .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap())
                    .format("%Y-%m-%d %H:%M:%S")
            ),
            Bson::Binary(bin) => {
                format!(
                    "'{}'",
                    base64::engine::general_purpose::STANDARD.encode(bin.bytes)
                )
            }
            _ => self
                .bson_value
                .as_str()
                .map(|s| format!("'{}'", Self::escape_string(s)))
                .unwrap_or_else(|| "' '".to_string()),
        }
    }

    fn to_date(self) -> String {
        format!(
            "toDate({})",
            self.bson_value.as_datetime().map_or("0".to_string(), |dt| {
                let utc = dt.timestamp_millis() / 1000;

                utc.to_string()
            })
        )
    }

    fn to_datetime(self) -> String {
        format!(
            "toDateTime({})",
            self.bson_value.as_datetime().map_or("0".to_string(), |dt| {
                let utc = dt.timestamp_millis() / 1000;

                utc.to_string()
            })
        )
    }

    fn to_time(self) -> String {
        format!(
            "toTime('{}')",
            self.bson_value
                .as_timestamp()
                .map_or("1970-01-01 00:00:00".to_string(), |ts| {
                    let datetime = chrono::DateTime::from_timestamp(ts.time as i64, 0)
                        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                    datetime.format("%Y-%m-%d %H:%M:%S").to_string()
                })
        )
    }

    fn to_array(self) -> String {
        if let Some(array) = self.bson_value.as_array() {
            match array.first().map(|v| v.element_type()) {
                Some(ElementType::Int32) | Some(ElementType::Int64) => {
                    let array_values = array
                        .iter()
                        .map(|v| v.as_i64().map_or("0".to_string(), |i| i.to_string()))
                        .collect::<Vec<String>>();

                    return format!("[{}]", array_values.join(", "));
                }
                Some(ElementType::Double) => {
                    let array_values = array
                        .iter()
                        .map(|v| v.as_f64().map_or("0.0".to_string(), |f| f.to_string()))
                        .collect::<Vec<String>>();

                    return format!("[{}]", array_values.join(", "));
                }
                Some(ElementType::String) => {
                    let array_values = array
                        .iter()
                        .map(|v| {
                            v.as_str()
                                .map(|s| format!("'{}'", Self::escape_string(s)))
                                .unwrap_or_else(|| "' '".to_string())
                        })
                        .collect::<Vec<String>>();

                    return format!("[{}]", array_values.join(", "));
                }
                _ => {}
            }
        }

        "[]".to_string()
    }

    fn to_string_array(self) -> String {
        if let Some(array) = self.bson_value.as_array() {
            let array_values = array
                .iter()
                .filter_map(|v| v.as_str().map(|s| format!("'{}'", Self::escape_string(s))))
                .collect::<Vec<String>>();

            return format!("[{}]", array_values.join(", "));
        }

        "[]".to_string()
    }

    fn is_null(&self) -> bool {
        matches!(
            self,
            Self {
                bson_value: Bson::Null,
                ..
            }
        )
    }

    fn unknown_value(self) -> String {
        "NULL".to_string()
    }

    fn into_null(self) -> Self {
        Self {
            bson_value: Bson::Null,
            ..self
        }
    }
}

impl MongoDBColumn {
    pub fn escape_string(input: &str) -> String {
        input.replace('\'', "''").replace("\\", "\\\\")
    }
}

impl IntoClickhouseColumn for MongoDBColumn {
    fn to_clickhouse_type(&self) -> ClickhouseType {
        match self.bson_value {
            Bson::String(_) => ClickhouseType::nullable(ClickhouseType::String),
            Bson::Array(_) => {
                ClickhouseType::nullable(ClickhouseType::Array(Box::new(ClickhouseType::Unknown)))
            }
            Bson::Document(_) => ClickhouseType::nullable(ClickhouseType::String),
            Bson::Boolean(_) => ClickhouseType::nullable(ClickhouseType::Bool),
            Bson::Null => ClickhouseType::nullable(ClickhouseType::Unknown),
            Bson::Int32(_) => ClickhouseType::nullable(ClickhouseType::Int32),
            Bson::Int64(_) => ClickhouseType::nullable(ClickhouseType::Int64),
            Bson::Double(_) => ClickhouseType::nullable(ClickhouseType::Float64),
            Bson::Decimal128(_) => ClickhouseType::nullable(ClickhouseType::Decimal),
            Bson::DateTime(_) => {
                ClickhouseType::nullable(ClickhouseType::DateTime(Default::default()))
            }
            Bson::Timestamp(_) => {
                ClickhouseType::nullable(ClickhouseType::DateTime(Default::default()))
            }
            Bson::Binary(_) => ClickhouseType::nullable(ClickhouseType::String),
            Bson::ObjectId(_) => {
                if self.column_name == "_id" {
                    ClickhouseType::String
                } else {
                    ClickhouseType::nullable(ClickhouseType::String)
                }
            }
            Bson::RegularExpression(_) => ClickhouseType::nullable(ClickhouseType::String),
            Bson::JavaScriptCode(_) => ClickhouseType::nullable(ClickhouseType::String),
            Bson::JavaScriptCodeWithScope(_) => ClickhouseType::nullable(ClickhouseType::String),
            Bson::Symbol(_) => ClickhouseType::nullable(ClickhouseType::String),
            Bson::Undefined => ClickhouseType::nullable(ClickhouseType::Unknown),
            Bson::MaxKey => ClickhouseType::nullable(ClickhouseType::String),
            Bson::MinKey => ClickhouseType::nullable(ClickhouseType::String),
            Bson::DbPointer(_) => ClickhouseType::nullable(ClickhouseType::String),
        }
    }

    fn get_column_name(&self) -> &str {
        &self.column_name
    }

    fn get_column_index(&self) -> usize {
        0_usize
    }

    fn get_comment(&self) -> &str {
        ""
    }

    fn is_in_primary_key(&self) -> bool {
        self.column_name == "_id"
    }
}

#[derive(Debug, Clone, Default)]
pub struct MongoDBCopyRow {
    pub columns: Vec<MongoDBColumn>,
}

impl IntoClickhouseRow for MongoDBCopyRow {
    fn find_value_by_column_name(
        &self,
        _: &[impl IntoClickhouseColumn],
        column_name: &str,
    ) -> Option<impl IntoClickhouseValue + Default> {
        for column in &self.columns {
            if column.column_name == column_name {
                return Some(column.clone());
            }
        }

        None
    }
}
