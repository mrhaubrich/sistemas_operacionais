use anyhow::{Context, Result};
use memmap2::Mmap;
use std::fs::File;
use std::path::Path;

/// Memory-mapped CSV file
pub struct MappedCsvFile {
    _file: File,
    pub mmap: Mmap,
    pub header: String,
    pub data_start_offset: usize,
}

impl MappedCsvFile {
    /// Map a CSV file into memory and parse the header
    pub fn new<P: AsRef<Path>>(file_path: P) -> Result<Self> {
        let file = File::open(&file_path)
            .with_context(|| format!("Failed to open file: {}", file_path.as_ref().display()))?;
        
        let mmap = unsafe { Mmap::map(&file) }
            .with_context(|| "Failed to memory map file")?;
        
        // Find the end of the first line (header)
        let data = std::str::from_utf8(&mmap)
            .with_context(|| "File contains invalid UTF-8")?;
        
        let header_end = data.find('\n')
            .ok_or_else(|| anyhow::anyhow!("File appears to be empty or has no newlines"))?;
        
        let header = data[..header_end].trim().to_string();
        let data_start_offset = header_end + 1;
        
        Ok(Self {
            _file: file,
            mmap,
            header,
            data_start_offset,
        })
    }
    
    /// Get the header as a vector of column names
    pub fn get_header_columns(&self, delimiter: char) -> Vec<String> {
        self.header
            .split(delimiter)
            .map(|col| col.trim().to_string())
            .collect()
    }
    
    /// Find the index of a specific column in the header
    pub fn find_column_index(&self, column_name: &str, delimiter: char) -> Option<usize> {
        let columns = self.get_header_columns(delimiter);
        columns.iter().position(|col| col == column_name)
    }
    
    /// Get the data portion of the file (everything after the header)
    pub fn get_data(&self) -> Result<&str> {
        let full_content = std::str::from_utf8(&self.mmap)
            .with_context(|| "File contains invalid UTF-8")?;
        
        Ok(&full_content[self.data_start_offset..])
    }
    
    /// Get the entire file content as a string
    #[allow(dead_code)]
    pub fn get_full_content(&self) -> Result<&str> {
        std::str::from_utf8(&self.mmap)
            .with_context(|| "File contains invalid UTF-8")
    }
    
    /// Validate that the file has a .csv extension
    pub fn validate_csv_extension<P: AsRef<Path>>(file_path: P) -> bool {
        file_path.as_ref()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_lowercase() == "csv")
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;
    
    #[test]
    fn test_csv_extension_validation() {
        assert!(MappedCsvFile::validate_csv_extension("test.csv"));
        assert!(MappedCsvFile::validate_csv_extension("test.CSV"));
        assert!(!MappedCsvFile::validate_csv_extension("test.txt"));
        assert!(!MappedCsvFile::validate_csv_extension("test"));
    }
    
    #[test]
    fn test_memory_mapping() -> Result<()> {
        // Create a temporary CSV file
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "id|device|temperature|humidity")?;
        writeln!(temp_file, "1|dev1|23.5|45.2")?;
        writeln!(temp_file, "2|dev2|24.1|46.8")?;
        
        let mapped = MappedCsvFile::new(temp_file.path())?;
        
        assert_eq!(mapped.header, "id|device|temperature|humidity");
        assert_eq!(mapped.find_column_index("device", '|'), Some(1));
        assert_eq!(mapped.find_column_index("nonexistent", '|'), None);
        
        let columns = mapped.get_header_columns('|');
        assert_eq!(columns, vec!["id", "device", "temperature", "humidity"]);
        
        Ok(())
    }
}
