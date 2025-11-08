use polars::prelude::*;
use std::fs::File;


pub fn write_parquet(df: &mut DataFrame, output_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let file = File::create(output_path)?;
    
    ParquetWriter::new(file)
        .finish(df)?;
    
    Ok(())
}
