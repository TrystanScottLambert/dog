// Module which handles reading the parquet file

use std::fs::File;
use polars::{frame::DataFrame, prelude::ParquetReader};
use polars::prelude::*;


pub fn read_parquet_file(file_name : &str) -> DataFrame {
    let mut file =  File::open(file_name).expect("Failed to open file");
    ParquetReader::new(&mut file).finish().expect("Failed to parse parquet.")
}
