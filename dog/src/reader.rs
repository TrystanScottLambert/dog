// Module which handles reading the parquet file

use std::{fs::File, process::exit};
use polars::{frame::DataFrame, prelude::ParquetReader};
use polars::prelude::*;


pub fn read_parquet_file(file_name : &str) -> DataFrame {
    let mut file =  match File::open(file_name) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening file: {e}");
            exit(1)
        }
    };

    ParquetReader::new(&mut file).finish().unwrap()
}
