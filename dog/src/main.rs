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


fn print_metadata(reader: &SerializedFileReader<File>) {
    let metadata = reader.metadata();
    println!("{:?}", metadata);
}

enum PrintFormat {
    Row,
    Column,
}

fn print_column_names(reader: &SerializedFileReader<File>, layout: PrintFormat) {
    let mut iterator = reader.get_row_iter(None).unwrap();
    let column_names: Vec<String> = iterator
        .next()
        .unwrap()
        .unwrap()
        .get_column_iter()
        .map(|(value, _)| format!("{}", value))
        .collect();
    match layout {
        PrintFormat::Column => println!("{}", column_names.join("\n")),
        PrintFormat::Row => println!("{}", column_names.join(" ")),
    };
    
}

fn print_columns_and_data(reader: SerializedFileReader<File>) {
    print_column_names(&reader, PrintFormat::Row);
    print_only_data(&reader);
}

fn print_tail(reader: &SerializedFileReader<File>) {
    let iterator = reader.get_row_iter(None).unwrap();
    let rows: Vec<_> = iterator.collect::<Result<_, _>>().unwrap();

    for row in rows.iter().rev().take(10).rev() {
        let values: Vec<String> = row
            .get_column_iter()
            .map(|(_, value)| format!("{}", value))
            .collect();
        println!("{}", values.join(" "));
    }
}


fn print_head(reader: SerializedFileReader<File>) {
    print_column_names(&reader, PrintFormat::Row);
    let iterator = reader.get_row_iter(None).unwrap();
    for row in iterator.take(10) {
        let values: Vec<String> = row
            .unwrap()
            .get_column_iter()
            .map(|(_, value)| format!("{}", value))
            .collect();
        println!("{}", values.join(" "))
    }
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
            Arg::new("names")
                .short('n')
                .long("names")
                .help("Prints only column names")
                .action(ArgAction::SetTrue)
                .conflicts_with("data"),
        )
        .arg(
            Arg::new("data")
                .short('d')
                .long("data")
                .help("Prints only the data")
                .action(ArgAction::SetTrue)
                .conflicts_with("names"),
        )
        .arg (
            Arg::new("tail")
            .short('t')
            .long("tail")
            .help("Prints the bottom ten rows of data")
            .action(ArgAction::SetTrue)
            .conflicts_with("head")
        )
        .arg(
            Arg::new("head")
            .short('H')
            .long("head")
            .help("Prints the top ten rows of data")
            .action(ArgAction::SetTrue)
            .conflicts_with("tail")
        )
        .arg(
            Arg::new("META")
            .short('M')
            .long("META")
            .help("Forcefully prints all metadata without any formatting.")
            .action(ArgAction::SetTrue)
        )
        .get_matches();

    let file = matches.get_one::<String>("file").expect("File argument missing");
    let reader = read_parquet_file(file);

    if *matches.get_one::<bool>("names").unwrap_or(&false) {
        print_column_names(&reader, PrintFormat::Column);
    } else if *matches.get_one::<bool>("data").unwrap_or(&false) {
        print_only_data(&reader);
    } else if *matches.get_one::<bool>("tail").unwrap_or(&false) {
        print_tail(&reader);
    } else if *matches.get_one::<bool>("head").unwrap_or(&false) {
        print_head(reader);
    } else if *matches.get_one::<bool>("META").unwrap_or(&false) {
        print_metadata(&reader);
    } else {
        print_columns_and_data(reader);
    }

    Ok(())
}
