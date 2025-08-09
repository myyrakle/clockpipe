#[derive(Debug)]
pub enum Errors {
    ConfigReadError(String),
    IOError(std::io::Error),
    DatabaseConnectionError(String),
    DatabasePingError(String),
}

pub type Result<T> = std::result::Result<T, Errors>;

impl From<std::io::Error> for Errors {
    fn from(err: std::io::Error) -> Self {
        Errors::IOError(err)
    }
}
