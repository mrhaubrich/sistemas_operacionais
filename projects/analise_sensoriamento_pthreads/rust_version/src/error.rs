use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum ProcessingError {
    #[error("File I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("CSV parsing error: {0}")]
    CsvParsing(String),

    #[error("Invalid file extension: expected .csv")]
    InvalidFileExtension,

    #[error("Column '{0}' not found in CSV header")]
    ColumnNotFound(String),

    #[error("Memory mapping error: {0}")]
    MemoryMapping(String),

    #[error("Data processing error: {0}")]
    DataProcessing(String),

    #[error("Polars error: {0}")]
    Polars(#[from] polars::error::PolarsError),

    #[error("Thread join error")]
    ThreadJoin,

    #[error("Channel communication error: {0}")]
    Channel(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Empty dataset: no data to process")]
    EmptyDataset,
}

impl From<crossbeam_channel::RecvError> for ProcessingError {
    fn from(err: crossbeam_channel::RecvError) -> Self {
        ProcessingError::Channel(err.to_string())
    }
}

impl<T> From<crossbeam_channel::SendError<T>> for ProcessingError {
    fn from(err: crossbeam_channel::SendError<T>) -> Self {
        ProcessingError::Channel(err.to_string())
    }
}
