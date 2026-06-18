use anyhow::Result;
use polars::prelude::*;
use std::{fs::File, path::PathBuf};

pub fn write_parquet(lazy_frame: LazyFrame, output_path: &PathBuf) -> Result<()> {
    let file = File::create(output_path)?;
    let mut df = lazy_frame.collect().unwrap();

    ParquetWriter::new(file).finish(&mut df)?;

    Ok(())
}

pub fn write_waves_metadata(
    lazy_frame: LazyFrame,
    output_path: &PathBuf,
    maml: String,
) -> Result<()> {
    let mut df = lazy_frame.clone().collect()?;
    let kv = KeyValueMetadata::from_static(vec![("maml".to_string(), maml.to_string())]);

    let file = File::create(output_path)?;
    ParquetWriter::new(file)
        .with_key_value_metadata(Some(kv))
        .finish(&mut df)?;
    Ok(())
}
