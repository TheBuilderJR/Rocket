use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::{ArrowReader, ArrowWriter, ParquetFileArrowReader};
use parquet::file::reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

pub fn serialize_to_parquet<T: Serialize>(data: &[T], file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let json = serde_json::to_string(&data)?;
    let value: Value = serde_json::from_str(&json)?;
    let schema = Schema::new(vec![Field::new("data", DataType::Json, false)]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(value)])?;
    let file = File::create(file_path)?;
    let writer = SerializedFileWriter::new(file, Arc::new(schema), Default::default())?;
    let mut arrow_writer = ArrowWriter::try_new(writer, Arc::new(schema), None)?;
    arrow_writer.write(&batch)?;
    arrow_writer.close()?;
    Ok(())
}

pub fn deserialize_from_parquet<T: for<'de> Deserialize<'de>>(file_path: &str) -> Result<Vec<T>, Box<dyn std::error::Error>> {
    let file = File::open(file_path)?;
    let reader = SerializedFileReader::new(file)?;
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(reader));
    let batch = arrow_reader.next().ok_or("No batch found")??;
    let json = serde_json::to_string(&batch.column(0))?;
    let data: Vec<T> = serde_json::from_str(&json)?;
    Ok(data)
}
