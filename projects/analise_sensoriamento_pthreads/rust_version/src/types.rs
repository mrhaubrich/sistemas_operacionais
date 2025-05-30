use ahash::AHashMap;
use std::sync::Arc;

/// Represents a slice of CSV data with associated metadata
#[derive(Debug, Clone)]
pub struct CsvChunk {
    /// The raw CSV data as a string slice
    pub data: String,
    /// CSV header for this chunk
    pub header: String,
    /// Device IDs included in this chunk
    pub device_ids: Vec<String>,
    /// Number of data lines (excluding header)
    pub line_count: usize,
}

/// Entry for a specific device containing all its data lines
#[derive(Debug, Clone)]
pub struct DeviceEntry {
    /// Device identifier
    pub device_id: String,
    /// All CSV lines for this device (without header)
    pub lines: Vec<String>,
}

/// Thread-safe hash table mapping device IDs to their data lines
pub type DeviceHashTable<'a> = AHashMap<String, Vec<&'a str>>;

/// Represents sensor data aggregations
#[derive(Debug, Clone)]
pub struct SensorAggregation {
    pub device: String,
    pub year_month: String,
    pub sensor: String,
    pub max_value: f64,
    pub mean_value: f64,
    pub min_value: f64,
}

/// Processed results from data analysis
#[derive(Debug)]
pub struct AnalysisResults {
    pub aggregations: Vec<SensorAggregation>,
    pub total_lines_processed: usize,
    pub processing_time_ms: f64,
}

/// Configuration for CSV processing
#[derive(Debug, Clone)]
pub struct ProcessingConfig {
    /// Path to the CSV file
    pub file_path: String,
    /// Name of the device column (default: "device")
    pub device_column: String,
    /// Number of worker threads/tasks
    pub num_workers: usize,
    /// CSV delimiter (default: "|")
    pub delimiter: char,
}

impl Default for ProcessingConfig {
    fn default() -> Self {
        Self {
            file_path: String::new(),
            device_column: "device".to_string(),
            num_workers: num_cpus::get(),
            delimiter: '|',
        }
    }
}

/// Statistics about the CSV file and processing
#[derive(Debug)]
pub struct ProcessingStats {
    pub total_lines: usize,
    pub unique_devices: usize,
    pub chunks_created: usize,
    pub mapping_time_ms: f64,
    pub hash_building_time_ms: f64,
    pub partitioning_time_ms: f64,
    pub processing_time_ms: f64,
    pub total_time_ms: f64,
}
