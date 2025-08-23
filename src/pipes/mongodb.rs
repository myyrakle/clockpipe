use crate::{
    adapter::{self, IntoClickhouse, clickhouse::ClickhouseColumn},
    config::Configuraion,
    errors::Errors,
    pipes::IPipe,
};

#[derive(Debug, Clone, Default)]
pub struct MongoDBPipeContext {}

impl MongoDBPipeContext {}

#[derive(Debug, Clone)]
pub struct MongoDBPipeTableInfo {
    clickhouse_columns: Vec<ClickhouseColumn>,
}

#[derive(Clone)]
pub struct MongoDBPipe {
    context: MongoDBPipeContext,

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
        // Since MongoDB is a schemaless DB, static setup is not possible.
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

            log::info!("Copying data from MongoDB table {collection_name}...",);
            // let rows = self
            //     .mongodb_connection
            //     .copy_table_to_stdout(&collection.schema_name, &collection.table_name)
            //     .await
            //     .expect("Failed to copy table data from MongoDB");

            // log::info!("Inserting copied data into ClickHouse table {schema_name}.{table_name}...",);

            // let source_table_info = self
            //     .context
            //     .tables_map
            //     .get(&format!("{schema_name}.{table_name}"))
            //     .expect("Table info not found in context");

            // let mask_columns = &collection.mask_columns;

            // let chunks = rows.chunks(100000);
            // let chunk_count = chunks.len();

            // for (chunk_index, chunk) in chunks.enumerate() {
            //     let chunk_index = chunk_index + 1;
            //     let percent = (chunk_index * 100) / chunk_count;

            //     log::debug!(
            //         "Processing chunk {percent}% ({chunk_index}/{chunk_count}) for table {schema_name}.{table_name}",
            //     );

            //     let insert_query = self.generate_insert_query(
            //         &self.clickhouse_config,
            //         &source_table_info.clickhouse_columns,
            //         &source_table_info.postgres_columns,
            //         mask_columns,
            //         &collection.table_name,
            //         chunk,
            //     );

            //     if !insert_query.is_empty() {
            //         self.clickhouse_connection
            //             .execute_query(&insert_query)
            //             .await
            //             .expect("Failed to execute insert query in ClickHouse");
            //     }
            // }

            log::info!("Copy completed for collection {collection_name}");
        }
    }

    async fn sync_loop(&self) {
        log::info!("Starting sync loop...");

        // 'SYNC_LOOP: loop {
        //     // 1. Peek new rows
        //     let peek_result = self
        //         .postgres_connection
        //         .peek_wal_changes(
        //             publication_name,
        //             replication_slot_name,
        //             self.config.peek_changes_limit,
        //         )
        //         .await;

        //     let peek_result = match peek_result {
        //         Ok(peek) => peek,
        //         Err(e) => {
        //             // 1.1. Handle peek error. wait and retry
        //             log::error!("Error peeking WAL changes: {e:?}");
        //             tokio::time::sleep(std::time::Duration::from_millis(
        //                 self.config.sleep_millis_when_peek_failed,
        //             ))
        //             .await;
        //             continue 'SYNC_LOOP;
        //         }
        //     };

        //     if peek_result.is_empty() {
        //         log::info!("No new changes found, waiting for next iteration...");
        //         tokio::time::sleep(std::time::Duration::from_millis(
        //             self.config.sleep_millis_when_peek_is_empty,
        //         ))
        //         .await;
        //         continue 'SYNC_LOOP;
        //     }

        //     let mut table_log_map = HashMap::new();

        //     let mut batch_insert_queue = HashMap::new();
        //     let mut batch_delete_queue = HashMap::new();

        //     // 2. Parse peeked rows, group by table and prepare for insert/update/delete
        //     for row in peek_result.iter() {
        //         let Some(parsed_row) =
        //             parse_pg_output(&row.data).expect("Failed to parse PgOutput")
        //         else {
        //             continue;
        //         };

        //         let Some((schema_name, table_name)) =
        //             self.context.table_relation_map.get(&parsed_row.relation_id)
        //         else {
        //             log::warn!(
        //                 "Relation ID {} not found in context table relation map",
        //                 parsed_row.relation_id
        //             );
        //             continue;
        //         };

        //         match parsed_row.message_type {
        //             MessageType::Insert | MessageType::Update => {
        //                 let table_info = self
        //                     .context
        //                     .tables_map
        //                     .get(&format!("{schema_name}.{table_name}"))
        //                     .expect("Table info not found in context");

        //                 let mask_columns = self
        //                     .postgres_config
        //                     .tables
        //                     .iter()
        //                     .find(|t| {
        //                         t.table_name == table_name.as_str()
        //                             && t.schema_name == schema_name.as_str()
        //                     })
        //                     .map_or_else(Vec::new, |t| t.mask_columns.clone());

        //                 batch_insert_queue
        //                     .entry(table_name)
        //                     .or_insert_with(|| BatchWriteEntry {
        //                         table_info,
        //                         mask_columns,
        //                         rows: Vec::new(),
        //                     })
        //                     .push(MongoDBCopyRow {
        //                         columns: parsed_row.payload,
        //                     });

        //                 let count = table_log_map
        //                     .entry(format!("{schema_name}.{table_name}"))
        //                     .or_insert(WriteCounter::default());

        //                 if parsed_row.message_type == MessageType::Insert {
        //                     count.insert_count += 1;
        //                 } else {
        //                     count.update_count += 1;
        //                 }
        //             }
        //             MessageType::Delete => {
        //                 let source_table_info = self
        //                     .context
        //                     .tables_map
        //                     .get(&format!("{schema_name}.{table_name}"))
        //                     .expect("Table info not found in context");

        //                 batch_delete_queue
        //                     .entry(table_name)
        //                     .or_insert_with(|| BatchWriteEntry {
        //                         table_info: source_table_info,
        //                         mask_columns: Vec::new(),
        //                         rows: Vec::new(),
        //                     })
        //                     .push(MongoDBCopyRow {
        //                         columns: parsed_row.payload,
        //                     });

        //                 let count = table_log_map
        //                     .entry(format!("{schema_name}.{table_name}"))
        //                     .or_insert(WriteCounter::default());

        //                 count.delete_count += 1;
        //             }
        //             MessageType::Truncate => {
        //                 // Truncate is handled separately, no need to queue

        //                 let database = &self.clickhouse_config.connection.database;

        //                 if let Err(error) = self
        //                     .clickhouse_connection
        //                     .truncate_table(database, table_name)
        //                     .await
        //                 {
        //                     log::error!(
        //                         "Failed to truncate table {}.{}: {}",
        //                         schema_name,
        //                         table_name,
        //                         error
        //                     );

        //                     tokio::time::sleep(std::time::Duration::from_millis(
        //                         self.config.sleep_millis_when_write_failed,
        //                     ))
        //                     .await;

        //                     continue 'SYNC_LOOP;
        //                 }

        //                 log::info!("Table {}.{} was truncated.", schema_name, table_name);
        //             }
        //             _ => {}
        //         }
        //     }

        //     // 3. Insert/Update rows in ClickHouse
        //     for (table_name, batch) in batch_insert_queue.iter() {
        //         let insert_query = self.generate_insert_query(
        //             &self.clickhouse_config,
        //             &batch.table_info.clickhouse_columns,
        //             &batch.table_info.postgres_columns,
        //             &batch.mask_columns,
        //             table_name,
        //             &batch.rows,
        //         );

        //         if !insert_query.is_empty() {
        //             if let Err(error) = self
        //                 .clickhouse_connection
        //                 .execute_query(&insert_query)
        //                 .await
        //             {
        //                 log::error!("Failed to execute insert query for {table_name}: {error}");
        //                 tokio::time::sleep(std::time::Duration::from_millis(
        //                     self.config.sleep_millis_when_write_failed,
        //                 ))
        //                 .await;

        //                 continue 'SYNC_LOOP;
        //             }

        //             tokio::time::sleep(std::time::Duration::from_millis(
        //                 self.config.sleep_millis_after_sync_write,
        //             ))
        //             .await;
        //         }
        //     }

        //     // 4. Delete rows in ClickHouse
        //     for (table_name, batch) in batch_delete_queue.iter() {
        //         let delete_query = self.generate_delete_query(
        //             &self.clickhouse_config,
        //             &batch.table_info.clickhouse_columns,
        //             &batch.table_info.postgres_columns,
        //             table_name,
        //             &batch.rows,
        //         );

        //         if !delete_query.is_empty() {
        //             if let Err(error) = self
        //                 .clickhouse_connection
        //                 .execute_query(&delete_query)
        //                 .await
        //             {
        //                 log::error!("Failed to execute delete query for {table_name}: {error}");
        //                 tokio::time::sleep(std::time::Duration::from_millis(
        //                     self.config.sleep_millis_when_write_failed,
        //                 ))
        //                 .await;

        //                 continue 'SYNC_LOOP;
        //             }

        //             tokio::time::sleep(std::time::Duration::from_millis(
        //                 self.config.sleep_millis_after_sync_write,
        //             ))
        //             .await;
        //         }
        //     }

        //     // 5. Move cursor for next peek
        //     if let Some(last) = peek_result.last() {
        //         let advance_key = &last.lsn;

        //         if let Err(e) = self
        //             .postgres_connection
        //             .advance_replication_slot(replication_slot_name, advance_key)
        //             .await
        //         {
        //             log::error!("Error advancing exporter: {e:?}");
        //             continue 'SYNC_LOOP;
        //         }
        //     }

        //     // 6. Log the changes
        //     for (table_name, count) in table_log_map.iter() {
        //         log::info!(
        //             "Table [{}]: Inserted: {}, Updated: {}, Deleted: {}",
        //             table_name,
        //             count.insert_count,
        //             count.update_count,
        //             count.delete_count
        //         );
        //     }

        //     tokio::time::sleep(std::time::Duration::from_millis(
        //         self.config.sleep_millis_after_sync_iteration,
        //     ))
        //     .await;
        // }
    }
}

impl MongoDBPipe {}

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
