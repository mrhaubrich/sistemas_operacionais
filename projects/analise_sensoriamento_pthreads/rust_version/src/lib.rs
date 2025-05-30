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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

#[test]
fn test_end_to_end_csv_processing() -> anyhow::Result<()> {
    // Create a test CSV file
    let mut temp_file = NamedTempFile::new()?;
    writeln!(temp_file, "id|device|data|temperatura|umidade|luminosidade|ruido|eco2|etvoc")?;
    writeln!(temp_file, "1|dev1|2024-04-01 10:00:00|23.5|45.0|100|50|400|200")?;
    writeln!(temp_file, "2|dev1|2024-04-01 11:00:00|24.0|46.0|110|55|410|210")?;
    writeln!(temp_file, "3|dev2|2024-04-01 12:00:00|22.5|44.0|95|48|395|195")?;
    writeln!(temp_file, "4|dev2|2024-04-01 13:00:00|23.0|45.5|105|52|405|205")?;
    
    // Test file mapping
    let mapped_file = MappedCsvFile::new(temp_file.path())?;
    assert!(mapped_file.header.contains("device"));
    
    let device_column_index = mapped_file.find_column_index("device", '|').unwrap();
    assert_eq!(device_column_index, 1);
    
    // Test hash table building
    let config = ProcessingConfig {
        delimiter: '|',
        ..Default::default()
    };
    
    let data = mapped_file.get_data()?;
    let hash_table = build_device_hash_table(data, device_column_index, &mapped_file.header, &config)?;
    
    assert_eq!(hash_table.len(), 2); // dev1 and dev2
    assert!(hash_table.contains_key("dev1"));
    assert!(hash_table.contains_key("dev2"));
    
    // Test partitioning
    let chunks = partition_by_device(&hash_table, 2, &mapped_file.header);
    assert!(!chunks.is_empty());
    assert!(chunks.len() <= 2);
    
    // Test parallel processing
    let results = process_chunks_parallel(chunks)?;
    assert!(!results.is_empty());
    
    // Verify we have some aggregations
    let total_aggregations: usize = results.iter().map(|r| r.aggregations.len()).sum();
    assert!(total_aggregations > 0);
    
    Ok(())
}

#[test]
fn test_csv_chunk_analysis() -> anyhow::Result<()> {
    let chunk = CsvChunk {
        data: "1|dev1|2024-04-01 10:00:00|23.5|45.0|100|50|400|200\n2|dev1|2024-04-01 11:00:00|24.0|46.0|110|55|410|210".to_string(),
        header: "id|device|data|temperatura|umidade|luminosidade|ruido|eco2|etvoc".to_string(),
        device_ids: vec!["dev1".to_string()],
        line_count: 2,
    };
    
    let result = analyze_csv_chunk(&chunk)?;
    
    // Should have aggregations for each sensor type
    assert!(!result.aggregations.is_empty());
    assert_eq!(result.total_lines_processed, 2);
    
    // Verify the aggregations contain expected data
    let has_temperatura = result.aggregations.iter().any(|agg| agg.sensor == "temperatura");
    assert!(has_temperatura, "Should have temperatura aggregations");
    
    Ok(())
}

#[test]
fn test_device_partitioning_load_balance() {
    use crate::device_hash::calculate_load_balance_stats;
    use dashmap::DashMap;
    use std::sync::Arc;
    
    let hash_table = Arc::new(DashMap::new());
    
    // Create uneven device data
    hash_table.insert("dev1".to_string(), vec!["line1".to_string(); 100]);
    hash_table.insert("dev2".to_string(), vec!["line2".to_string(); 50]);
    hash_table.insert("dev3".to_string(), vec!["line3".to_string(); 25]);
    
    let chunks = partition_by_device(&hash_table, 3, "header");
    
    // Should create chunks that attempt to balance load
    assert_eq!(chunks.len(), 3);
    
    let (imbalance_ratio, min_load, max_load) = calculate_load_balance_stats(&chunks);
    assert!(min_load > 0);
    assert!(max_load >= min_load);
    assert!(imbalance_ratio >= 0.0);
}

#[test]
fn test_empty_data_handling() -> anyhow::Result<()> {
    let chunk = CsvChunk {
        data: String::new(),
        header: "id|device|data|temperatura".to_string(),
        device_ids: vec![],
        line_count: 0,
    };
    
    let result = analyze_csv_chunk(&chunk)?;
    assert_eq!(result.aggregations.len(), 0);
    assert_eq!(result.total_lines_processed, 0);
    
    Ok(())
}

#[test]
fn test_csv_validation() {
    assert!(MappedCsvFile::validate_csv_extension("test.csv"));
    assert!(MappedCsvFile::validate_csv_extension("test.CSV"));
    assert!(!MappedCsvFile::validate_csv_extension("test.txt"));
    assert!(!MappedCsvFile::validate_csv_extension("test"));
}

}
