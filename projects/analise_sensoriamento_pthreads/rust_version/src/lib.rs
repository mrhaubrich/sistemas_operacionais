// Re-export modules for external use
pub mod data_analysis;
pub mod device_hash;
pub mod error;
pub mod file_mapping;
pub mod parallel_processor;
pub mod types;

pub use data_analysis::analyze_csv_chunk;
pub use device_hash::{build_device_hash_table, partition_by_device};
pub use file_mapping::MappedCsvFile;
pub use parallel_processor::process_chunks_parallel;
pub use types::{CsvChunk, ProcessingConfig};
