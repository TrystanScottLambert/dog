// Module which handles reading the parquet file

use std::{fs::File, process::exit, path::Path};
use parquet::file::reader::SerializedFileReader;

pub fn read_parquet_file(file_name: &str) -> SerializedFileReader<File> {
    let file = match File::open(Path::new(file_name)) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error opening file: {}", e);
            exit(1);
        }
    };

    match SerializedFileReader::new(file) {
        Ok(reader) => reader,
        Err(e) => {
            eprintln!("Error reading parquet file: {e}");
            exit(1);
        }
    }
}
