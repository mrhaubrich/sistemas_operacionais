mod data_analysis;
mod device_hash;
mod error;
mod file_mapping;
mod parallel_processor;
mod types;

use anyhow::{Context, Result};
use clap::Parser;
use std::fs::File;
use std::io::Write;
use std::time::Instant;

use crate::data_analysis::results_to_csv;
use crate::device_hash::{build_device_hash_table, calculate_load_balance_stats, partition_by_device};
use crate::file_mapping::MappedCsvFile;
use crate::parallel_processor::{print_processing_stats, process_chunks_parallel, WorkStealingProcessor};
use crate::types::{ProcessingConfig, ProcessingStats};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the CSV file to process
    file_path: String,
    
    /// Name of the device column (default: "device")
    #[arg(short, long, default_value = "device")]
    device_column: String,
    
    /// Number of worker threads (default: number of CPU cores)
    #[arg(short, long)]
    num_workers: Option<usize>,
    
    /// CSV delimiter character (default: "|")
    #[arg(long, default_value = "|")]
    delimiter: char,
    
    /// Use async processing instead of parallel processing
    #[arg(long)]
    use_async: bool,
    
    /// Use work-stealing processor instead of rayon
    #[arg(long)]
    use_work_stealing: bool,
    
    /// Output file path (default: "result.csv")
    #[arg(short, long, default_value = "result.csv")]
    output: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    
    println!("🦀 Análise de Sensoriamento - Rust Version 🦀");
    println!("===============================================");
    
    let total_start = Instant::now();
    
    // Validate file extension
    if !MappedCsvFile::validate_csv_extension(&args.file_path) {
        return Err(anyhow::anyhow!("Invalid file extension. Expected .csv file"));
    }
    
    // Print system information
    let num_processors = num_cpus::get();
    let num_workers = args.num_workers.unwrap_or(num_processors);
    
    println!("[SYSTEM] Available CPU cores: {}", num_processors);
    println!("[SYSTEM] Using {} worker threads", num_workers);
    println!("[SYSTEM] Processing mode: {}", 
        if args.use_async { "Async" } 
        else if args.use_work_stealing { "Work-stealing" }
        else { "Rayon parallel" }
    );
    
    // Create processing configuration
    let config = ProcessingConfig {
        file_path: args.file_path.clone(),
        device_column: args.device_column.clone(),
        num_workers,
        delimiter: args.delimiter,
    };
    
    // Phase 1: Memory mapping
    println!("\n[PHASE 1] Memory mapping CSV file...");
    let mapping_start = Instant::now();
    
    let mapped_file = MappedCsvFile::new(&args.file_path)
        .with_context(|| format!("Failed to map file: {}", args.file_path))?;
    
    let mapping_time = mapping_start.elapsed();
    println!("[PHASE 1] ✅ File mapped in {:.2}ms", mapping_time.as_millis());
    
    // Find device column index
    let device_column_index = mapped_file
        .find_column_index(&args.device_column, args.delimiter)
        .ok_or_else(|| anyhow::anyhow!("Device column '{}' not found", args.device_column))?;
    
    println!("[PHASE 1] Device column '{}' found at index {}", 
        args.device_column, device_column_index);
    
    // Phase 2: Build device hash table
    println!("\n[PHASE 2] Building device hash table...");
    let hash_start = Instant::now();
    
    let data = mapped_file.get_data()
        .with_context(|| "Failed to get CSV data")?;
    
    let device_hash_table = build_device_hash_table(
        data,
        device_column_index,
        &mapped_file.header,
        &config,
    ).with_context(|| "Failed to build device hash table")?;
    
    let hash_time = hash_start.elapsed();
    
    // Count total lines
    let total_lines: usize = device_hash_table.iter().map(|entry| entry.value().len()).sum();
    let unique_devices = device_hash_table.len();
    
    println!("[PHASE 2] ✅ Hash table built in {:.2}ms", hash_time.as_millis());
    println!("[PHASE 2] Total data lines: {}", total_lines);
    println!("[PHASE 2] Unique devices: {}", unique_devices);
    
    if unique_devices == 0 {
        return Err(anyhow::anyhow!("No devices found in the dataset"));
    }
    
    // Phase 3: Partition data by device
    println!("\n[PHASE 3] Partitioning data by device...");
    let partition_start = Instant::now();
    
    let chunks = partition_by_device(&device_hash_table, num_workers, &mapped_file.header);
    let partition_time = partition_start.elapsed();
    
    println!("[PHASE 3] ✅ Data partitioned in {:.2}ms", partition_time.as_millis());
    println!("[PHASE 3] Created {} chunks", chunks.len());
    
    // Print load balancing statistics
    let (imbalance_ratio, min_load, max_load) = calculate_load_balance_stats(&chunks);
    println!("[PHASE 3] Load balance - Min: {} lines, Max: {} lines, Imbalance ratio: {:.2}", 
        min_load, max_load, imbalance_ratio);
    
    if chunks.is_empty() {
        return Err(anyhow::anyhow!("No data chunks created"));
    }
    
    // Phase 4: Parallel processing
    println!("\n[PHASE 4] Processing data chunks in parallel...");
    let processing_start = Instant::now();
    
    let results = if args.use_async {
        // Use async processing
        let rt = tokio::runtime::Runtime::new()
            .with_context(|| "Failed to create async runtime")?;
        rt.block_on(crate::parallel_processor::process_chunks_async(chunks))
            .with_context(|| "Async processing failed")?
    } else if args.use_work_stealing {
        // Use work-stealing processor
        let processor = WorkStealingProcessor::new(num_workers);
        processor.process_chunks(chunks)
            .with_context(|| "Work-stealing processing failed")?
    } else {
        // Use rayon parallel processing (default)
        process_chunks_parallel(chunks)
            .with_context(|| "Parallel processing failed")?
    };
    
    let processing_time = processing_start.elapsed();
    println!("[PHASE 4] ✅ All chunks processed in {:.2}s", processing_time.as_secs_f64());
    
    // Print detailed processing statistics
    print_processing_stats(&results);
    
    // Phase 5: Generate output
    println!("[PHASE 5] Writing results to file...");
    let output_start = Instant::now();
    
    let csv_output = results_to_csv(&results);
    
    let mut output_file = File::create(&args.output)
        .with_context(|| format!("Failed to create output file: {}", args.output))?;
    
    output_file.write_all(csv_output.as_bytes())
        .with_context(|| "Failed to write CSV output")?;
    
    let output_time = output_start.elapsed();
    println!("[PHASE 5] ✅ Results written to '{}' in {:.2}ms", args.output, output_time.as_millis());
    
    // Calculate total aggregations
    let total_aggregations: usize = results.iter().map(|r| r.aggregations.len()).sum();
    println!("[PHASE 5] Total aggregations written: {}", total_aggregations);
    
    let total_time = total_start.elapsed();
    
    // Final performance summary
    println!("\n🏁 PERFORMANCE SUMMARY 🏁");
    println!("========================");
    println!("File mapping:           {:.2}ms", mapping_time.as_millis());
    println!("Hash table building:    {:.2}ms", hash_time.as_millis());
    println!("Data partitioning:      {:.2}ms", partition_time.as_millis());
    println!("Parallel processing:    {:.2}ms", processing_time.as_millis());
    println!("Output writing:         {:.2}ms", output_time.as_millis());
    println!("------------------------");
    println!("TOTAL EXECUTION TIME:   {:.2}s", total_time.as_secs_f64());
    
    // Throughput calculations
    if total_time.as_secs_f64() > 0.0 {
        let lines_per_second = total_lines as f64 / total_time.as_secs_f64();
        let aggregations_per_second = total_aggregations as f64 / total_time.as_secs_f64();
        
        println!("------------------------");
        println!("Processing throughput:  {:.2} lines/second", lines_per_second);
        println!("Aggregation rate:       {:.2} aggregations/second", aggregations_per_second);
    }
    
    println!("========================");
    println!("🎉 Processing completed successfully! 🎉");
    
    Ok(())
}
