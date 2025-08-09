#[derive(Debug)]
pub enum Errors {
    ConfigReadError(String),
    IOError(std::io::Error),
    DatabaseConnectionError(String),
    DatabasePingError(String),
    TableNotFoundError(String),
    GetTableNameFailed(String),
    PublicationCreateFailed(String),
    PublicationAddFailed(String),
    ReplicationCreateFailed(String),
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
            Errors::ConfigReadError(msg) => write!(f, "Configuration read error: {}", msg),
            Errors::IOError(err) => write!(f, "I/O error: {}", err),
            Errors::DatabaseConnectionError(msg) => write!(f, "Database connection error: {}", msg),
            Errors::DatabasePingError(msg) => write!(f, "Database ping error: {}", msg),
            Errors::TableNotFoundError(msg) => write!(f, "Table not found: {}", msg),
            Errors::GetTableNameFailed(msg) => write!(f, "Failed to get table name: {}", msg),
            Errors::PublicationCreateFailed(msg) => {
                write!(f, "Failed to create publication: {}", msg)
            }
            Errors::PublicationAddFailed(msg) => {
                write!(f, "Failed to add table to publication: {}", msg)
            }
            Errors::ReplicationCreateFailed(msg) => {
                write!(f, "Failed to create replication slot: {}", msg)
            }
        }
    }
}
