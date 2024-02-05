use datafusion::prelude::*;
use parquet::arrow::ArrowReader;
use parquet::file::reader::SerializedFileReader;
use std::fs::File;
use std::sync::Arc;
use arrow::util::pretty::print_batches;
use std::error::Error;

pub struct SqlHandler {
    data_path: String,
}

impl SqlHandler {
    pub fn new(data_path: String) -> Self {
        SqlHandler { data_path }
    }

    pub async fn execute_query<T: for<'de> serde::Deserialize<'de>>(&self, query: &str) -> Result<Vec<T>, Box<dyn Error>> {
        let ctx = SessionContext::new();
        let file = File::open(&self.data_path)?;
        let reader = SerializedFileReader::new(file)?;
        let arrow_reader = Arc::new(ParquetFileArrowReader::new(Arc::new(reader)));
        let schema = arrow_reader.get_schema()?;
        let batch = arrow_reader.get_record_reader(2048)?.next().ok_or("No batch found")??;

        ctx.register_parquet("data", &self.data_path, ParquetReadOptions::default()).await?;
        let df = ctx.sql(query).await?;
        let results = df.collect().await?;

        print_batches(&results)?;

        let json = serde_json::to_string(&results)?;
        let data: Vec<T> = serde_json::from_str(&json)?;
        Ok(data)
    }
}
