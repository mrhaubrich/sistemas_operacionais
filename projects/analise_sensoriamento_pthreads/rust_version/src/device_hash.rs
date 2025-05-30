use crate::types::{CsvChunk, DeviceHashTable, ProcessingConfig};
use ahash::AHashMap;
use anyhow::Result;

/// Build a hash table organizing CSV data by device
pub fn build_device_hash_table<'a>(
    data: &'a str,
    device_column_index: usize,
    _header: &str,
    config: &ProcessingConfig,
) -> Result<DeviceHashTable<'a>> {
    let estimated_lines = data.lines().count();
    let mut hash_table: DeviceHashTable<'a> = AHashMap::with_capacity(estimated_lines / 2);
    let delimiter = config.delimiter as u8;
    for line in data.lines() {
        let line = line; // no trim for performance
        if line.is_empty() {
            continue;
        }
        let bytes = line.as_bytes();
        let mut col_start = 0;
        let mut col_end = 0;
        let mut col_idx = 0;
        let mut found = false;
        for (i, &b) in bytes.iter().enumerate() {
            if b == delimiter {
                if col_idx == device_column_index {
                    col_end = i;
                    found = true;
                    break;
                }
                col_idx += 1;
                col_start = i + 1;
            }
        }
        if !found {
            // If last column
            if col_idx == device_column_index {
                col_end = bytes.len();
                found = true;
            }
        }
        if !found {
            eprintln!("Warning: Line has insufficient fields: {}", line);
            continue;
        }
        // SAFETY: line is valid UTF-8, so this slice is too
        let device_id = unsafe { std::str::from_utf8_unchecked(&bytes[col_start..col_end]) };
        if device_id.is_empty() {
            continue;
        }
        hash_table
            .entry(device_id.to_string())
            .or_insert_with(Vec::new)
            .push(line);
    }
    Ok(hash_table)
}

/// Partition devices across workers to balance load
pub fn partition_by_device<'a>(
    hash_table: &DeviceHashTable<'a>,
    num_workers: usize,
    header: &str,
) -> Vec<CsvChunk> {
    if hash_table.is_empty() || num_workers == 0 {
        return Vec::new();
    }

    // Collect all devices with their line counts
    let mut devices_with_counts: Vec<(String, usize)> = hash_table
        .iter()
        .map(|(k, v)| (k.clone(), v.len()))
        .collect();

    // Sort by line count (descending) to enable better load balancing
    devices_with_counts.sort_by(|a, b| b.1.cmp(&a.1));

    // Initialize workers with empty chunks
    let mut worker_chunks: Vec<CsvChunk> = (0..num_workers)
        .map(|_| CsvChunk {
            data: String::new(),
            header: header.to_string(),
            device_ids: Vec::new(),
            line_count: 0,
        })
        .collect();

    // Track current load for each worker
    let mut worker_loads = vec![0usize; num_workers];

    // Distribute devices using a greedy approach (assign to least loaded worker)
    for (device_id, line_count) in devices_with_counts {
        // Find the worker with the minimum current load
        let min_worker_idx = worker_loads
            .iter()
            .enumerate()
            .min_by_key(|(_, &load)| load)
            .map(|(idx, _)| idx)
            .unwrap_or(0);

        // Add device data to the selected worker's chunk
        if let Some(device_lines) = hash_table.get(&device_id) {
            let chunk = &mut worker_chunks[min_worker_idx];
            for line in device_lines.iter() {
                if !chunk.data.is_empty() {
                    chunk.data.push('\n');
                }
                chunk.data.push_str(line);
            }
            chunk.device_ids.push(device_id);
            chunk.line_count += line_count;
            worker_loads[min_worker_idx] += line_count;
        }
    }

    // Filter out empty chunks
    worker_chunks
        .into_iter()
        .filter(|chunk| chunk.line_count > 0)
        .collect()
}

/// Calculate load balancing statistics
pub fn calculate_load_balance_stats(chunks: &[CsvChunk]) -> (f64, usize, usize) {
    if chunks.is_empty() {
        return (0.0, 0, 0);
    }

    let loads: Vec<usize> = chunks.iter().map(|chunk| chunk.line_count).collect();
    let min_load = *loads.iter().min().unwrap();
    let max_load = *loads.iter().max().unwrap();
    let avg_load = loads.iter().sum::<usize>() as f64 / loads.len() as f64;

    let imbalance_ratio = if avg_load > 0.0 {
        (max_load as f64 - min_load as f64) / avg_load
    } else {
        0.0
    };

    (imbalance_ratio, min_load, max_load)
}
