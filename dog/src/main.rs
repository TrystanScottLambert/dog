use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::process::exit;

fn main() -> parquet::errors::Result<()> {
    let infile = String::from("../../waves_wide_missingcols_bp0p1p0.parquet");
    let file = match File::open(Path::new(&infile)) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("File not found! Exiting with following error {}", e);
            exit(1);
        }
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(reader) => reader,
        Err(e) => {
            eprintln!("Something went wrong reading parquet file: {}", e);
            exit(1);
        }
    };

    let mut iterator = reader.get_row_iter(None).unwrap();
    while let Some(row) = iterator.next() {
        println!("{:?}", row.unwrap())
    };
    
    Ok(())
}
