use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::process::exit;

fn print_all_data(reader: SerializedFileReader<File>) {
    // prints all the column data.
    let mut iterator = reader.get_row_iter(None).unwrap();
    while let Some(row) = iterator.next() {
        let values: Vec<String> = row
            .unwrap()
            .get_column_iter()
            .map(|(_, value)| format!("{}", value))
            .collect();
        println!("{}", values.join(" "));
    };
}

fn print_columns(reader: SerializedFileReader<File>) {
    // Only prints the column information.
    let mut iterator = reader.get_row_iter(None).unwrap();
    let column_names: Vec<String> = iterator
        .next()
        .unwrap()
        .unwrap()
        .get_column_iter()
        .map(|(value, _)| format!("{}", value))
        .collect();

    println!("{}", column_names.join("\n"))
}

fn read_parquet_file(file_name: String) -> SerializedFileReader<File> {
    // reads the parquet file and creates a reader object for analysis.
    let file = match File::open(Path::new(&file_name)) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("Error. File might not exist. Exiting with error {}", e);
            exit(1);
        }
    };

    let reader = match SerializedFileReader::new(file) {
        Ok(reader) => reader,
        Err(e) => {
            eprintln!("Error converting to Readable format: {e}");
            exit(1);
        }
    };
    reader
}

fn main() -> parquet::errors::Result<()> {
    let infile = String::from("../../waves_wide_missingcols_bp0p1p0.parquet");
    let reader = read_parquet_file(infile);
    print_columns(reader);
    Ok(())
}
