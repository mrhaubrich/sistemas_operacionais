use crate::types::{AnalysisResults, CsvChunk, SensorAggregation};
use anyhow::{Context, Result};
use chrono::NaiveDate;
use polars::prelude::*;
use std::time::Instant;

/// Analyze a CSV chunk using Polars (pure Rust replacement for Python script)
pub fn analyze_csv_chunk(chunk: &CsvChunk) -> Result<AnalysisResults> {
    let start_time = Instant::now();

    if chunk.data.is_empty() {
        return Ok(AnalysisResults {
            aggregations: Vec::new(),
            total_lines_processed: 0,
            processing_time_ms: start_time.elapsed().as_millis() as f64,
        });
    }

    // Create CSV content with header
    let csv_content = format!("{}\n{}", chunk.header, chunk.data);

    // Parse CSV manually since Polars scan_csv is not available for in-memory data
    let lines: Vec<&str> = csv_content.lines().collect();

    if lines.is_empty() {
        return Ok(AnalysisResults {
            aggregations: Vec::new(),
            total_lines_processed: 0,
            processing_time_ms: start_time.elapsed().as_millis() as f64,
        });
    }

    // Get header and data lines
    let header_line = lines[0];
    let data_lines = &lines[1..];

    if data_lines.is_empty() {
        return Ok(AnalysisResults {
            aggregations: Vec::new(),
            total_lines_processed: 0,
            processing_time_ms: start_time.elapsed().as_millis() as f64,
        });
    }

    // Parse header to get column names
    let column_names: Vec<&str> = header_line.split('|').collect();

    // Parse data rows, skipping malformed lines and lines that look like JSON or CSV artifacts
    let mut columns: Vec<Vec<AnyValue>> = vec![Vec::new(); column_names.len()];

    for line in data_lines {
        let values: Vec<&str> = line.split('|').collect();
        if values.len() != column_names.len() {
            // Skip malformed lines
            continue;
        }
        // Additional filter: skip lines that look like JSON (start with '{' or '[')
        if let Some(first) = values[0].chars().next() {
            if first == '{' || first == '[' {
                continue;
            }
        }
        // Extra filter: skip lines where all columns are numeric or empty (likely artifact rows)
        if values
            .iter()
            .all(|v| v.trim().is_empty() || v.trim().parse::<f64>().is_ok())
        {
            continue;
        }
        // Extra filter: skip lines where any column contains 'device_id', 'device_name', or 'variable' (likely JSON keys)
        if values
            .iter()
            .any(|v| v.contains("device_id") || v.contains("device_name") || v.contains("variable"))
        {
            continue;
        }
        for (i, &value) in values.iter().enumerate() {
            columns[i].push(AnyValue::String(value));
        }
    }

    // Create Series for each column
    let mut series_vec = Vec::new();
    for (i, &col_name) in column_names.iter().enumerate() {
        if i < columns.len() {
            let series = Series::new(col_name, &columns[i]);
            series_vec.push(series);
        }
    }

    // Create DataFrame
    let df =
        DataFrame::new(series_vec).with_context(|| "Failed to create DataFrame from parsed CSV")?;

    // Apply the same transformations as the Python script
    let processed_df = process_dataframe(df)?;

    // Convert to our result format
    let aggregations = dataframe_to_aggregations(processed_df)?;

    let processing_time = start_time.elapsed().as_millis() as f64;

    Ok(AnalysisResults {
        aggregations,
        total_lines_processed: chunk.line_count,
        processing_time_ms: processing_time,
    })
}

/// Process dataframe with the same logic as the Python script
fn process_dataframe(mut df: DataFrame) -> Result<DataFrame> {
    // Drop unnecessary columns if they exist
    let columns_to_drop = ["id", "latitude", "longitude"];
    let existing_columns: Vec<String> = df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();

    for col_name in &columns_to_drop {
        if existing_columns.contains(&col_name.to_string()) {
            df = df
                .drop(col_name)
                .with_context(|| format!("Failed to drop column: {}", col_name))?;
        }
    }

    // Drop rows with null values (specify columns explicitly)
    let all_columns: Vec<&str> = df.get_column_names();
    df = df
        .drop_nulls(Some(&all_columns))
        .with_context(|| "Failed to drop null values")?;

    // Check if device column exists
    if !df.get_column_names().contains(&"device") {
        println!("Warning: DataFrame does not contain 'device' column");
        return create_empty_result_dataframe();
    }

    // Convert to LazyFrame for better API support
    let mut lf = df.lazy();

    // Filter out rows where device is null (matching Python script behavior)
    lf = lf.filter(col("device").is_not_null());

    // Collect to check if empty
    let df = lf
        .collect()
        .with_context(|| "Failed to collect after device filter")?;

    if df.height() == 0 {
        println!("Warning: DataFrame is empty after normalization");
        return create_empty_result_dataframe();
    }

    // Convert back to LazyFrame for transformations
    let mut lf = df.lazy();

    // Overwrite the 'data' column with parsed Datetime (matching Python script)
    lf = lf.with_columns([col("data")
        .str()
        .split(lit(" "))
        .list()
        .get(lit(0), true)
        .str()
        .strptime(
            DataType::Datetime(TimeUnit::Milliseconds, None),
            StrptimeOptions {
                format: Some("%Y-%m-%d".to_string()),
                ..Default::default()
            },
            lit("raise"),
        )
        .alias("data")]);

    // Filter dates after 2024-03-01 (inclusive)
    let filter_date = NaiveDate::from_ymd_opt(2024, 3, 1)
        .ok_or_else(|| anyhow::anyhow!("Invalid filter date"))?;
    let filter_datetime = filter_date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| anyhow::anyhow!("Invalid filter datetime"))?;
    let filter_timestamp = filter_datetime.and_utc().timestamp_millis();

    lf = lf.filter(col("data").gt_eq(lit(filter_timestamp)));

    // Add year-month column
    lf = lf.with_columns([col("data").dt().strftime("%Y-%m").alias("ano-mes")]);

    // Get sensor columns
    let df_temp = lf
        .clone()
        .collect()
        .with_context(|| "Failed to collect for sensor detection")?;
    let sensor_columns: Vec<&str> = [
        "temperatura",
        "umidade",
        "luminosidade",
        "ruido",
        "eco2",
        "etvoc",
    ]
    .iter()
    .filter(|&col| df_temp.get_column_names().contains(col))
    .copied()
    .collect();

    if sensor_columns.is_empty() {
        println!("Warning: No sensor columns found");
        return create_empty_result_dataframe();
    }

    // Cast sensor columns to Float64 (only if not already Float64, matching Python script)
    let mut cast_expressions = Vec::new();
    for &sensor in &sensor_columns {
        let column = df_temp
            .column(sensor)
            .with_context(|| format!("Failed to get column: {}", sensor))?;
        // Only cast if not already Float64
        if column.dtype() != &DataType::Float64 {
            cast_expressions.push(col(sensor).cast(DataType::Float64));
        } else {
            cast_expressions.push(col(sensor)); // Keep as is
        }
    }

    if !cast_expressions.is_empty() {
        lf = lf.with_columns(cast_expressions);
    }

    // Drop rows with nulls after casting sensor columns (to match Python Polars behavior)
    let all_columns_after_cast: Vec<Expr> =
        lf.schema()?.iter_fields().map(|f| col(f.name())).collect();
    lf = lf.drop_nulls(Some(all_columns_after_cast));

    // Process each sensor and create aggregations
    let mut result_dfs = Vec::new();

    for &sensor in &sensor_columns {
        let sensor_lf = lf
            .clone()
            .group_by([col("device"), col("ano-mes")])
            .agg([
                col(sensor).max().alias("valor_maximo"),
                col(sensor).mean().alias("valor_medio"),
                col(sensor).min().alias("valor_minimo"),
            ])
            .with_columns([lit(sensor).alias("sensor")]);

        result_dfs.push(sensor_lf);
    }

    // Concatenate all sensor results
    let final_result = concat(result_dfs, UnionArgs::default())
        .with_context(|| "Failed to concatenate sensor results")?
        .sort(
            ["device", "ano-mes", "sensor"],
            SortMultipleOptions::default(),
        )
        .select([
            col("device"),
            col("ano-mes"),
            col("sensor"),
            col("valor_maximo"),
            col("valor_medio"),
            col("valor_minimo"),
        ]);

    final_result
        .collect()
        .with_context(|| "Failed to collect final results")
}

/// Create an empty result dataframe with the correct schema
fn create_empty_result_dataframe() -> Result<DataFrame> {
    let schema = Schema::from_iter(vec![
        Field::new("device", DataType::String),
        Field::new("ano-mes", DataType::String),
        Field::new("sensor", DataType::String),
        Field::new("valor_maximo", DataType::Float64),
        Field::new("valor_medio", DataType::Float64),
        Field::new("valor_minimo", DataType::Float64),
    ]);

    Ok(DataFrame::empty_with_schema(&schema))
}

/// Convert a Polars DataFrame to our SensorAggregation format
fn dataframe_to_aggregations(df: DataFrame) -> Result<Vec<SensorAggregation>> {
    let mut aggregations = Vec::new();

    let height = df.height();
    if height == 0 {
        return Ok(aggregations);
    }

    // Extract columns
    let device_col = df
        .column("device")
        .with_context(|| "Missing device column")?
        .str()
        .with_context(|| "Device column is not string type")?;

    let year_month_col = df
        .column("ano-mes")
        .with_context(|| "Missing ano-mes column")?
        .str()
        .with_context(|| "ano-mes column is not string type")?;

    let sensor_col = df
        .column("sensor")
        .with_context(|| "Missing sensor column")?
        .str()
        .with_context(|| "Sensor column is not string type")?;

    let max_col = df
        .column("valor_maximo")
        .with_context(|| "Missing valor_maximo column")?
        .f64()
        .with_context(|| "valor_maximo column is not f64 type")?;

    let mean_col = df
        .column("valor_medio")
        .with_context(|| "Missing valor_medio column")?
        .f64()
        .with_context(|| "valor_medio column is not f64 type")?;

    let min_col = df
        .column("valor_minimo")
        .with_context(|| "Missing valor_minimo column")?
        .f64()
        .with_context(|| "valor_minimo column is not f64 type")?;

    // Convert each row to SensorAggregation
    for i in 0..height {
        let device = device_col
            .get(i)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let year_month = year_month_col
            .get(i)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let sensor = sensor_col
            .get(i)
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());

        let max_value = max_col.get(i).unwrap_or(0.0);
        let mean_value = mean_col.get(i).unwrap_or(0.0);
        let min_value = min_col.get(i).unwrap_or(0.0);

        aggregations.push(SensorAggregation {
            device,
            year_month,
            sensor,
            max_value,
            mean_value,
            min_value,
        });
    }

    Ok(aggregations)
}

/// Convert analysis results to CSV format
pub fn results_to_csv(results: &[AnalysisResults]) -> String {
    let mut csv_output =
        String::from("device,ano-mes,sensor,valor_maximo,valor_medio,valor_minimo\n");

    for result in results {
        for agg in &result.aggregations {
            csv_output.push_str(&format!(
                "{},{},{},{},{},{}\n",
                agg.device,
                agg.year_month,
                agg.sensor,
                agg.max_value,
                agg.mean_value,
                agg.min_value
            ));
        }
    }

    csv_output
}
