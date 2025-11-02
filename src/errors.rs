#[derive(Debug)]
pub enum Errors {
    ConfigReadError(String),
    IOError(std::io::Error),
    ClickhouseDatabaseError(String),
    DatabaseConnectionError(String),
    DatabaseQueryError(String),
    DatabasePingError(String),
    TableNotFoundError(String),
    GetTableNameFailed(String),
    ListTableColumnsFailed(String),
    PublicationCreateFailed(String),
    PublicationAddFailed(String),
    PublicationFindFailed(String),
    ReplicationCreateFailed(String),
    ReplicationNotFound(String),
    PeekChangesFailed(String),
    ReplicationSlotAdvanceFailed(String),
    PgOutputParseError(String),
    CopyTableFailed(String),
    CountTableRowsFailed(String),
    ResumeTokenParseError(String),
}

pub type Result<T> = std::result::Result<T, Errors>;

impl From<std::io::Error> for Errors {
    fn from(err: std::io::Error) -> Self {
        Errors::IOError(err)
    }
}

impl std::fmt::Display for Errors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Errors::ConfigReadError(msg) => write!(f, "Configuration read error: {msg}"),
            Errors::IOError(err) => write!(f, "I/O error: {err}"),
            Errors::DatabaseQueryError(msg) => write!(f, "Database query error: {msg}"),
            Errors::DatabaseConnectionError(msg) => write!(f, "Database connection error: {msg}"),
            Errors::DatabasePingError(msg) => write!(f, "Database ping error: {msg}"),
            Errors::TableNotFoundError(msg) => write!(f, "Table not found: {msg}"),
            Errors::GetTableNameFailed(msg) => write!(f, "Failed to get table name: {msg}"),
            Errors::PublicationCreateFailed(msg) => {
                write!(f, "Failed to create publication: {msg}")
            }
            Errors::PublicationAddFailed(msg) => {
                write!(f, "Failed to add table to publication: {msg}")
            }
            Errors::PublicationFindFailed(msg) => write!(f, "Publication not found: {msg}"),
            Errors::ReplicationCreateFailed(msg) => {
                write!(f, "Failed to create replication slot: {msg}")
            }
            Errors::ReplicationNotFound(msg) => write!(f, "Replication slot not found: {msg}"),
            Errors::PeekChangesFailed(msg) => write!(f, "Failed to peek changes: {msg}"),
            Errors::ReplicationSlotAdvanceFailed(msg) => {
                write!(f, "Failed to advance replication slot: {msg}")
            }
            Errors::PgOutputParseError(msg) => write!(f, "Failed to parse PgOutput: {msg}"),
            Errors::ListTableColumnsFailed(msg) => {
                write!(f, "Failed to list table columns: {msg}")
            }
            Errors::ClickhouseDatabaseError(msg) => write!(f, "ClickHouse database error: {msg}"),
            Errors::CopyTableFailed(msg) => write!(f, "Failed to copy table data: {msg}"),
            Errors::CountTableRowsFailed(msg) => write!(f, "Failed to count table rows: {msg}"),
            Errors::ResumeTokenParseError(msg) => write!(f, "Failed to parse resume token: {msg}"),
        }
    }
}
