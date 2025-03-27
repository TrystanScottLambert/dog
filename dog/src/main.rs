use clap::{Arg, ArgAction, Command};
use parquet::file::reader::{FileReader, SerializedFileReader};
use std::fs::File;
use std::path::Path;
use std::process::exit;

fn print_only_data(reader: &SerializedFileReader<File>) {
    let mut iterator = reader.get_row_iter(None).unwrap();
    while let Some(row) = iterator.next() {
        let values: Vec<String> = row
            .unwrap()
            .get_column_iter()
            .map(|(_, value)| format!("{}", value))
            .collect();
        println!("{}", values.join(" "));
    }
}

fn print_column_names(reader: &SerializedFileReader<File>) {
    let mut iterator = reader.get_row_iter(None).unwrap();
    let column_names: Vec<String> = iterator
        .next()
        .unwrap()
        .unwrap()
        .get_column_iter()
        .map(|(value, _)| format!("{}", value))
        .collect();
    println!("{}", column_names.join("\n"));
}

fn print_columns_and_data(reader: SerializedFileReader<File>) {
    print_column_names(&reader);
    print_only_data(&reader);
}

fn read_parquet_file(file_name: &str) -> SerializedFileReader<File> {
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

fn main() -> parquet::errors::Result<()> {
    let matches = Command::new("dog")
        .about("Parquet File Reader CLI")
        .arg(
            Arg::new("file")
                .required(true)
                .help("Input parquet file"),
        )
        .arg(
            Arg::new("columns")
                .short('c')
                .long("columns")
                .help("Prints only column names")
                .action(ArgAction::SetTrue) // 👈 Fix: Explicitly set action
                .conflicts_with("data"),
        )
        .arg(
            Arg::new("data")
                .short('d')
                .long("data")
                .help("Prints only the data")
                .action(ArgAction::SetTrue) // 👈 Fix: Explicitly set action
                .conflicts_with("columns"),
        )
        .get_matches();

    let file = matches.get_one::<String>("file").expect("File argument missing");
    let reader = read_parquet_file(file);

    if *matches.get_one::<bool>("columns").unwrap_or(&false) {
        print_column_names(&reader);
    } else if *matches.get_one::<bool>("data").unwrap_or(&false) {
        print_only_data(&reader);
    } else {
        print_columns_and_data(reader);
    }

    Ok(())
}
