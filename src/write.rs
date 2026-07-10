use anyhow::Result;
use polars::prelude::*;
use std::{fs::File, path::PathBuf};

pub fn write_parquet(lazy_frame: &LazyFrame, output_path: &PathBuf) -> Result<()> {
    let file = File::create(output_path)?;
    let mut df = lazy_frame.clone().collect().unwrap();

    ParquetWriter::new(file).finish(&mut df)?;

    Ok(())
}
