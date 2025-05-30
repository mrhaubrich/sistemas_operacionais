use crate::data_analysis::analyze_csv_chunk;
use crate::types::{AnalysisResults, CsvChunk};
use anyhow::Result;
use crossbeam_channel;
use rayon::prelude::*;
use std::time::Instant;
use tokio::sync::mpsc;

/// Process multiple CSV chunks in parallel using Rayon
pub fn process_chunks_parallel(chunks: Vec<CsvChunk>) -> Result<Vec<AnalysisResults>> {
    let start_time = Instant::now();

    println!("[PARALLEL] Processing {} chunks in parallel", chunks.len());

    // Use rayon to process chunks in parallel
    let results: Result<Vec<AnalysisResults>> = chunks
        .into_par_iter()
        .enumerate()
        .map(|(idx, chunk)| {
            println!(
                "[WORKER {}] Processing chunk with {} lines",
                idx, chunk.line_count
            );

            let chunk_start = Instant::now();
            let result = analyze_csv_chunk(&chunk);
            let chunk_time = chunk_start.elapsed();

            match &result {
                Ok(analysis_result) => {
                    println!(
                        "[WORKER {}] Completed in {:.2}ms - {} aggregations generated",
                        idx,
                        chunk_time.as_millis(),
                        analysis_result.aggregations.len()
                    );
                }
                Err(e) => {
                    eprintln!("[WORKER {}] Error: {}", idx, e);
                }
            }

            result
        })
        .collect();

    let total_time = start_time.elapsed();
    println!(
        "[PARALLEL] All chunks processed in {:.2}s",
        total_time.as_secs_f64()
    );

    results
}

/// Async version using Tokio tasks (alternative approach)
pub async fn process_chunks_async(chunks: Vec<CsvChunk>) -> Result<Vec<AnalysisResults>> {
    let start_time = Instant::now();
    let chunk_count = chunks.len();

    println!("[ASYNC] Processing {} chunks asynchronously", chunk_count);

    // Create a channel for results
    let (tx, mut rx) = mpsc::unbounded_channel();

    // Spawn a task for each chunk
    let handles: Vec<_> = chunks
        .into_iter()
        .enumerate()
        .map(|(idx, chunk)| {
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || {
                println!(
                    "[ASYNC WORKER {}] Processing chunk with {} lines",
                    idx, chunk.line_count
                );

                let chunk_start = Instant::now();
                let result = analyze_csv_chunk(&chunk);
                let chunk_time = chunk_start.elapsed();

                match &result {
                    Ok(analysis_result) => {
                        println!(
                            "[ASYNC WORKER {}] Completed in {:.2}ms - {} aggregations generated",
                            idx,
                            chunk_time.as_millis(),
                            analysis_result.aggregations.len()
                        );
                    }
                    Err(e) => {
                        eprintln!("[ASYNC WORKER {}] Error: {}", idx, e);
                    }
                }

                // Send result back
                let _ = tx.send((idx, result));
            })
        })
        .collect();

    // Drop the original sender so the receiver knows when all tasks are done
    drop(tx);

    // Collect results in order
    let mut results: Vec<Option<Result<AnalysisResults>>> =
        (0..chunk_count).map(|_| None).collect();
    while let Some((idx, result)) = rx.recv().await {
        results[idx] = Some(result);
    }

    // Wait for all tasks to complete
    for handle in handles {
        handle.await?;
    }

    let total_time = start_time.elapsed();
    println!(
        "[ASYNC] All chunks processed in {:.2}s",
        total_time.as_secs_f64()
    );

    // Convert Option<Result<...>> to Result<Vec<...>>
    results
        .into_iter()
        .enumerate()
        .map(|(idx, opt_result)| {
            opt_result.unwrap_or_else(|| Err(anyhow::anyhow!("Task {} did not complete", idx)))
        })
        .collect()
}

/// Work-stealing queue implementation using crossbeam channels
pub struct WorkStealingProcessor {
    workers: usize,
}

impl WorkStealingProcessor {
    pub fn new(workers: usize) -> Self {
        Self { workers }
    }

    pub fn process_chunks(self, chunks: Vec<CsvChunk>) -> Result<Vec<AnalysisResults>> {
        let start_time = Instant::now();
        let chunk_count = chunks.len();

        println!(
            "[WORK_STEALING] Processing {} chunks with {} workers",
            chunk_count, self.workers
        );

        if chunks.is_empty() {
            return Ok(Vec::new());
        }

        // Use crossbeam channels for work distribution
        let (work_sender, work_receiver) = crossbeam_channel::unbounded();
        let (result_sender, result_receiver) = crossbeam_channel::unbounded();

        // Send all chunks to the work queue
        for (idx, chunk) in chunks.into_iter().enumerate() {
            work_sender.send((idx, chunk)).unwrap();
        }
        drop(work_sender); // Signal no more work

        // Spawn worker threads
        let mut handles = Vec::new();
        for worker_id in 0..self.workers {
            let work_receiver = work_receiver.clone();
            let result_sender = result_sender.clone();

            let handle = std::thread::spawn(move || {
                let mut processed_chunks = 0;

                while let Ok((chunk_idx, chunk)) = work_receiver.recv() {
                    println!(
                        "[WORKER {}] Processing chunk {} with {} lines",
                        worker_id, chunk_idx, chunk.line_count
                    );

                    let chunk_start = Instant::now();
                    let result = analyze_csv_chunk(&chunk);
                    let chunk_time = chunk_start.elapsed();

                    match &result {
                        Ok(analysis_result) => {
                            println!(
                                "[WORKER {}] Chunk {} completed in {:.2}ms - {} aggregations",
                                worker_id,
                                chunk_idx,
                                chunk_time.as_millis(),
                                analysis_result.aggregations.len()
                            );
                        }
                        Err(e) => {
                            eprintln!("[WORKER {}] Chunk {} error: {}", worker_id, chunk_idx, e);
                        }
                    }

                    result_sender.send((chunk_idx, result)).unwrap();
                    processed_chunks += 1;
                }

                println!(
                    "[WORKER {}] Completed {} chunks",
                    worker_id, processed_chunks
                );
            });

            handles.push(handle);
        }

        drop(result_sender); // Signal no more results

        // Collect results
        let mut results: Vec<Option<Result<AnalysisResults>>> =
            (0..chunk_count).map(|_| None).collect();
        while let Ok((idx, result)) = result_receiver.recv() {
            results[idx] = Some(result);
        }

        // Wait for all workers to finish
        for handle in handles {
            handle.join().unwrap();
        }

        let total_time = start_time.elapsed();
        println!(
            "[WORK_STEALING] All chunks processed in {:.2}s",
            total_time.as_secs_f64()
        );

        // Convert to final result
        results
            .into_iter()
            .enumerate()
            .map(|(idx, opt_result)| {
                opt_result
                    .unwrap_or_else(|| Err(anyhow::anyhow!("Chunk {} was not processed", idx)))
            })
            .collect()
    }
}

/// Calculate and print processing statistics
pub fn print_processing_stats(results: &[AnalysisResults]) {
    if results.is_empty() {
        println!("[STATS] No results to analyze");
        return;
    }

    let total_lines: usize = results.iter().map(|r| r.total_lines_processed).sum();
    let total_aggregations: usize = results.iter().map(|r| r.aggregations.len()).sum();
    let total_processing_time: f64 = results.iter().map(|r| r.processing_time_ms).sum();
    let avg_processing_time = total_processing_time / results.len() as f64;

    let min_processing_time = results
        .iter()
        .map(|r| r.processing_time_ms)
        .fold(f64::INFINITY, f64::min);

    let max_processing_time = results
        .iter()
        .map(|r| r.processing_time_ms)
        .fold(f64::NEG_INFINITY, f64::max);

    println!("\n[STATS] ====== Processing Statistics ======");
    println!("[STATS] Total chunks processed: {}", results.len());
    println!("[STATS] Total lines processed: {}", total_lines);
    println!(
        "[STATS] Total aggregations generated: {}",
        total_aggregations
    );
    println!(
        "[STATS] Total processing time: {:.2}ms",
        total_processing_time
    );
    println!(
        "[STATS] Average processing time per chunk: {:.2}ms",
        avg_processing_time
    );
    println!(
        "[STATS] Fastest chunk processing: {:.2}ms",
        min_processing_time
    );
    println!(
        "[STATS] Slowest chunk processing: {:.2}ms",
        max_processing_time
    );

    if total_processing_time > 0.0 {
        let throughput = total_lines as f64 / (total_processing_time / 1000.0);
        println!(
            "[STATS] Processing throughput: {:.2} lines/second",
            throughput
        );
    }

    println!("[STATS] =====================================\n");
}
