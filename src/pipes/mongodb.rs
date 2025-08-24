use std::collections::HashMap;

use itertools::Itertools;
use mongodb::change_stream::event::OperationType;

use crate::{
    adapter::{
        self, IntoClickhouse,
        clickhouse::ClickhouseColumn,
        mongodb::{MongoDBColumn, MongoDBCopyRow},
    },
    config::Configuraion,
    errors::Errors,
    pipes::{IPipe, WriteCounter},
};

#[derive(Debug, Clone, Default)]
pub struct MongoDBPipeContext {
    pub tables_map: std::collections::HashMap<String, MongoDBPipeTableInfo>,
}

impl MongoDBPipeContext {
    pub fn new() -> Self {
        Self {
            tables_map: std::collections::HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MongoDBPipeTableInfo {
    clickhouse_columns: Vec<ClickhouseColumn>,
}

#[derive(Clone)]
pub struct MongoDBPipe {
    context: MongoDBPipeContext,

    #[allow(dead_code)]
    config: Configuraion,

    mongodb_config: crate::config::MongoDBConfig,
    mongodb_connection: adapter::mongodb::MongoDBConnection,

    clickhouse_config: crate::config::ClickHouseConfig,
    clickhouse_connection: adapter::clickhouse::ClickhouseConnection,
}

impl MongoDBPipe {
    pub async fn new(
        config: Configuraion,
        mongodb_config: crate::config::MongoDBConfig,
        clickhouse_config: crate::config::ClickHouseConfig,
    ) -> Self {
        let mongodb_connection = adapter::mongodb::MongoDBConnection::new(&mongodb_config)
            .await
            .expect("Failed to create MongoDB connection");

        let clickhouse_connection =
            adapter::clickhouse::ClickhouseConnection::new(&clickhouse_config.connection);

        MongoDBPipe {
            context: MongoDBPipeContext::default(),
            config,
            mongodb_config,
            clickhouse_config,
            mongodb_connection,
            clickhouse_connection,
        }
    }
}

#[async_trait::async_trait]
impl IPipe for MongoDBPipe {
    async fn ping(&self) -> Result<(), Errors> {
        self.mongodb_connection
            .ping()
            .await
            .map_err(|e| Errors::DatabasePingError(format!("MongoDB ping failed: {e}")))?;

        self.clickhouse_connection
            .ping()
            .await
            .map_err(|e| Errors::DatabasePingError(format!("ClickHouse ping failed: {e}")))?;

        log::info!("MongoDB and ClickHouse connections are healthy.");

        Ok(())
    }

    async fn initialize(&mut self) {
        log::info!("Initializing MongoDB Pipe...");

        self.setup_table()
            .await
            .expect("Failed to setup ClickHouse table");
    }

    async fn first_sync(&self) {
        log::info!("Starting initial sync...");

        for collection in &self.mongodb_config.collections {
            let collection_name = &collection.collection_name;

            if collection.skip_copy {
                log::debug!(
                    "Skipping initial sync for {collection_name} as skip_copy is set to true"
                );
                continue;
            }

            if self
                .clickhouse_connection
                .table_is_not_empty(
                    &self.clickhouse_config.connection.database,
                    &collection.collection_name,
                )
                .await
                .expect("Failed to check if table exists")
            {
                log::debug!(
                    "Collection {collection_name} already exists in ClickHouse, skipping initial sync.",
                );
                continue;
            }

            log::info!("Copying data from MongoDB collection {collection_name}...",);

            let rows = self
                .mongodb_connection
                .copy_collection(
                    &self.mongodb_config.connection.database,
                    &collection.collection_name,
                )
                .await
                .expect("Failed to copy collection data from MongoDB");

            log::info!(
                "Fetched {} rows from MongoDB collection {collection_name}",
                rows.len()
            );

            self.add_columns_to_table_if_not_exists(&collection.collection_name, &rows)
                .await
                .expect("Failed to add columns to ClickHouse table if not exists");

            log::info!("Inserting copied data into ClickHouse table {collection_name}...",);

            let source_table_info = self
                .context
                .tables_map
                .get(&collection.collection_name)
                .expect("Table info not found in context");

            let mask_columns = &collection.mask_columns;

            let chunks = rows.chunks(100000);
            let chunk_count = chunks.len();

            for (chunk_index, chunk) in chunks.enumerate() {
                let chunk_index = chunk_index + 1;
                let percent = (chunk_index * 100) / chunk_count;

                log::debug!(
                    "Processing chunk {percent}% ({chunk_index}/{chunk_count}) for collection {collection_name}"
                );

                let insert_query = self.generate_insert_query(
                    &self.clickhouse_config,
                    &source_table_info.clickhouse_columns,
                    &Vec::<MongoDBColumn>::new(), // MongoDB does not have a fixed schema, so we pass an empty slice here
                    mask_columns,
                    &collection.collection_name,
                    chunk,
                );

                if !insert_query.is_empty() {
                    self.clickhouse_connection
                        .execute_query(&insert_query)
                        .await
                        .expect("Failed to execute insert query in ClickHouse");
                }
            }

            log::info!("Copy completed for collection {collection_name}");
        }
    }

    async fn sync_loop(&mut self) {
        log::info!("Starting sync loop...");

        'SYNC_LOOP: loop {
            // 1. Peek new rows
            let peek_result = self
                .mongodb_connection
                .peek_changes(
                    &self.mongodb_config.connection.database,
                    &self
                        .mongodb_config
                        .collections
                        .iter()
                        .map(|c| c.collection_name.as_str())
                        .collect::<Vec<&str>>(),
                    self.config.peek_changes_limit,
                    self.mongodb_config.peek_timeout_millis,
                )
                .await;

            let peek_result = match peek_result {
                Ok(peek) => peek,
                Err(e) => {
                    // 1.1. Handle peek error. wait and retry
                    log::error!("Error peeking stream changes: {e:?}");
                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_when_peek_failed,
                    ))
                    .await;
                    continue 'SYNC_LOOP;
                }
            };

            if peek_result.changes.is_empty() {
                log::info!("No new changes found, waiting for next iteration...");
                tokio::time::sleep(std::time::Duration::from_millis(
                    self.config.sleep_millis_when_peek_is_empty,
                ))
                .await;
                continue 'SYNC_LOOP;
            }

            // group by collection_name
            let chunk_iter = peek_result
                .changes
                .into_iter()
                .filter(|change| {
                    change.operation_type == OperationType::Insert
                        || change.operation_type == OperationType::Update
                })
                .chunk_by(|change| change.collection_name.clone());

            let mut changes_by_collection = HashMap::new();
            for (collection_name, group) in &chunk_iter {
                changes_by_collection.insert(collection_name.clone(), group.collect::<Vec<_>>());
            }

            for (collection_name, rows) in &changes_by_collection {
                let copy_rows = rows
                    .into_iter()
                    .map(|change| change.to_copy_row().unwrap_or_default())
                    .collect::<Vec<_>>();

                if let Err(error) = self
                    .add_columns_to_table_if_not_exists(&collection_name, &copy_rows)
                    .await
                {
                    log::error!(
                        "Failed to add columns to ClickHouse table {}: {}",
                        collection_name,
                        error
                    );

                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_when_write_failed,
                    ))
                    .await;

                    continue 'SYNC_LOOP;
                }

                if let Err(error) = self.load_table_table_info(&collection_name).await {
                    log::error!(
                        "Failed to reload table info for ClickHouse table {}: {}",
                        collection_name,
                        error
                    );

                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_when_write_failed,
                    ))
                    .await;

                    continue 'SYNC_LOOP;
                }
            }

            let mut table_log_map = HashMap::new();

            let mut batch_insert_queue = HashMap::new();
            let mut batch_delete_queue: HashMap<String, BatchWriteEntry<'_>> = HashMap::new();

            // 2. Parse peeked rows, group by table and prepare for insert/update/delete
            for (collection_name, rows) in changes_by_collection {
                for row in rows {
                    let copy_row = row.to_copy_row().unwrap_or_default();

                    match row.operation_type {
                        OperationType::Insert | OperationType::Update => {
                            let table_info = self
                                .context
                                .tables_map
                                .get(&collection_name)
                                .expect("Table info not found in context");

                            let mask_columns = self
                                .mongodb_config
                                .collections
                                .iter()
                                .find(|t| t.collection_name == collection_name.as_str())
                                .map_or_else(Vec::new, |t| t.mask_columns.clone());

                            println!("copy row: {:?}", copy_row);

                            batch_insert_queue
                                .entry(collection_name.clone())
                                .or_insert_with(|| BatchWriteEntry {
                                    table_info,
                                    mask_columns,
                                    rows: Vec::new(),
                                })
                                .push(copy_row);

                            let count: &mut WriteCounter = table_log_map
                                .entry(collection_name.clone())
                                .or_insert(WriteCounter::default());

                            if row.operation_type == OperationType::Insert {
                                count.insert_count += 1;
                            } else {
                                count.update_count += 1;
                            }
                        }
                        OperationType::Delete => {
                            let source_table_info = self
                                .context
                                .tables_map
                                .get(&collection_name)
                                .expect("Table info not found in context");

                            batch_delete_queue
                                .entry(collection_name.clone())
                                .or_insert_with(|| BatchWriteEntry {
                                    table_info: source_table_info,
                                    mask_columns: Vec::new(),
                                    rows: Vec::new(),
                                })
                                .push(copy_row);

                            let count = table_log_map
                                .entry(collection_name.clone())
                                .or_insert(WriteCounter::default());

                            count.delete_count += 1;
                        }
                        _ => {}
                    }
                }
            }

            // 3. Insert/Update rows in ClickHouse
            for (table_name, batch) in batch_insert_queue.iter() {
                let insert_query = self.generate_insert_query(
                    &self.clickhouse_config,
                    &batch.table_info.clickhouse_columns,
                    &Vec::<MongoDBColumn>::new(), // MongoDB does not have a fixed schema, so we pass an empty slice here
                    &batch.mask_columns,
                    table_name,
                    &batch.rows,
                );

                if !insert_query.is_empty() {
                    if let Err(error) = self
                        .clickhouse_connection
                        .execute_query(&insert_query)
                        .await
                    {
                        log::error!("Failed to execute insert query for {table_name}: {error}");
                        tokio::time::sleep(std::time::Duration::from_millis(
                            self.config.sleep_millis_when_write_failed,
                        ))
                        .await;

                        continue 'SYNC_LOOP;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_after_sync_write,
                    ))
                    .await;
                }
            }

            // 4. Delete rows in ClickHouse
            for (table_name, batch) in batch_delete_queue.iter() {
                let delete_query = self.generate_delete_query(
                    &self.clickhouse_config,
                    &batch.table_info.clickhouse_columns,
                    &Vec::<MongoDBColumn>::new(), // MongoDB does not have a fixed schema, so we pass an empty slice here
                    table_name,
                    &batch.rows,
                );

                if !delete_query.is_empty() {
                    if let Err(error) = self
                        .clickhouse_connection
                        .execute_query(&delete_query)
                        .await
                    {
                        log::error!("Failed to execute delete query for {table_name}: {error}");
                        tokio::time::sleep(std::time::Duration::from_millis(
                            self.config.sleep_millis_when_write_failed,
                        ))
                        .await;

                        continue 'SYNC_LOOP;
                    }

                    tokio::time::sleep(std::time::Duration::from_millis(
                        self.config.sleep_millis_after_sync_write,
                    ))
                    .await;
                }
            }

            // 5. Move cursor for next peek
            if let Err(error) = self
                .mongodb_connection
                .store_resume_token(&peek_result.resume_token)
            {
                log::error!("Failed to store resume token: {error}");
                tokio::time::sleep(std::time::Duration::from_millis(
                    self.config.sleep_millis_when_write_failed,
                ))
                .await;

                continue 'SYNC_LOOP;
            }

            // 6. Log the changes
            for (table_name, count) in table_log_map.iter() {
                log::info!(
                    "Table [{}]: Inserted: {}, Updated: {}, Deleted: {}",
                    table_name,
                    count.insert_count,
                    count.update_count,
                    count.delete_count
                );
            }

            tokio::time::sleep(std::time::Duration::from_millis(
                self.config.sleep_millis_after_sync_iteration,
            ))
            .await;
        }
    }
}

impl MongoDBPipe {
    async fn setup_table(&mut self) -> Result<(), Errors> {
        log::info!("Setting up tables in ClickHouse...");

        let collections = self.mongodb_config.collections.clone();

        for collection in &collections {
            let clickhouse_table_not_exists = self
                .clickhouse_connection
                .list_columns_by_tablename(
                    &self.clickhouse_config.connection.database,
                    &collection.collection_name,
                )
                .await?
                .is_empty();

            if clickhouse_table_not_exists {
                log::info!(
                    "Table {}.{} does not exist in ClickHouse, creating it",
                    &self.clickhouse_config.connection.database,
                    collection.collection_name
                );

                let create_table_query = self.generate_create_table_query(
                    &self.clickhouse_config.connection.database,
                    &collection.collection_name,
                    &[MongoDBColumn {
                        column_name: "_id".to_string(),
                        bson_value: mongodb::bson::Bson::ObjectId(
                            mongodb::bson::oid::ObjectId::new(),
                        ),
                        ..Default::default()
                    }],
                );

                self.clickhouse_connection
                    .execute_query(&create_table_query)
                    .await?;

                log::info!(
                    "Table {}.{} created in ClickHouse",
                    &self.clickhouse_config.connection.database,
                    collection.collection_name,
                );
            }

            self.load_table_table_info(&collection.collection_name)
                .await?;
        }

        Ok(())
    }

    async fn load_table_table_info(&mut self, table_name: &str) -> Result<(), Errors> {
        let clickhouse_columns = self
            .clickhouse_connection
            .list_columns_by_tablename(&self.clickhouse_config.connection.database, table_name)
            .await?;

        self.context.tables_map.insert(
            table_name.to_string(),
            MongoDBPipeTableInfo { clickhouse_columns },
        );

        Ok(())
    }

    async fn add_columns_to_table_if_not_exists(
        &self,
        collection_name: &str,
        rows: &[MongoDBCopyRow],
    ) -> Result<(), Errors> {
        let mut columns_to_add = vec![];
        let clickhouse_columns: &MongoDBPipeTableInfo = self
            .context
            .tables_map
            .get(collection_name)
            .expect("Table info not found in context");

        for row in rows {
            for column in &row.columns {
                if !clickhouse_columns
                    .clickhouse_columns
                    .iter()
                    .any(|c| c.column_name == column.column_name)
                    && !columns_to_add
                        .iter()
                        .any(|c: &MongoDBColumn| c.column_name == column.column_name)
                {
                    columns_to_add.push(column.clone());
                }
            }
        }

        for column_to_add in columns_to_add {
            let add_column_query = self.generate_add_column_query(
                &self.clickhouse_config,
                collection_name,
                &column_to_add,
            );

            self.clickhouse_connection
                .execute_query(&add_column_query)
                .await?;

            log::info!(
                "Added column {} to ClickHouse table {}",
                column_to_add.column_name,
                collection_name
            );
        }

        Ok(())
    }
}

impl IntoClickhouse for MongoDBPipe {}

pub async fn run_mongodb_pipe(config: Configuraion) {
    let mut pipe = MongoDBPipe::new(
        config.clone(),
        config.source.mongodb.expect("MongoDB config is required"),
        config
            .target
            .clickhouse
            .expect("Clickhouse config is required"),
    )
    .await;

    if let Err(error) = pipe.ping().await {
        log::error!("Failed to ping MongoDB exporter: {error:?}");
        return;
    }

    tokio::select! {
        _ = pipe.run_pipe() => {
            log::info!("MongoDB pipe running...");
        }
    }
}

pub struct BatchWriteEntry<'a> {
    pub table_info: &'a MongoDBPipeTableInfo,
    pub mask_columns: Vec<String>,
    pub rows: Vec<MongoDBCopyRow>,
}

impl BatchWriteEntry<'_> {
    pub fn push(&mut self, row: MongoDBCopyRow) {
        self.rows.push(row);
    }
}
