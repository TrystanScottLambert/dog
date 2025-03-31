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

fn print_selected_columns(reader: &SerializedFileReader<File>, columns: Vec<String>) {
    let mut iterator = reader.get_row_iter(None).unwrap();

    // Get column names from the first row
    let first_row = iterator.next().unwrap().unwrap();
    let column_names: Vec<String> = first_row
        .get_column_iter()
        .map(|(name, _)| name.to_string())
        .collect();

    // Determine which columns to extract (indices)
    let selected_indices: Vec<usize> = columns
        .iter()
        .filter_map(|col| {
            if let Ok(idx) = col.parse::<usize>() {
                if idx < column_names.len() {
                    Some(idx) // Column index case
                } else {
                    None // Ignore invalid indices
                }
            } else {
                column_names.iter().position(|name| name == col) // Column name case
            }
        })
        .collect();

    if selected_indices.is_empty() {
        eprintln!("No valid columns selected!");
        exit(1);
    }

    // Print selected column headers
    println!("{}", selected_indices.iter().map(|&i| column_names[i].clone()).collect::<Vec<String>>().join(" "));

    // Print selected column data for each row
    for row in reader.get_row_iter(None).unwrap() {
        let row = row.unwrap();
        let values: Vec<String> = row
            .get_column_iter()
            .enumerate()
            .filter_map(|(idx, (_, value))| {
                if selected_indices.contains(&idx) {
                    Some(format!("{}", value))
                } else {
                    None
                }
            })
            .collect();
        println!("{}", values.join(" "));
    }
}
 

fn print_summary(reader: &SerializedFileReader<File>) {
    let mut iterator = reader.get_row_iter(None).unwrap();
    
    // Get column names
    let first_row = iterator.next().unwrap().unwrap();
    let column_names: Vec<String> = first_row.get_column_iter().map(|(name, _)| name.to_string()).collect();
    
    // Store column data
    let mut column_data: Vec<Vec<String>> = vec![vec![]; column_names.len()];
    let mut row_count = 1;  // First row already read

    for row in iterator {
        let row = row.unwrap();
        for (i, (_, value)) in row.get_column_iter().enumerate() {
            if column_data[i].len() < 5 {  // Limit to 5 samples
                column_data[i].push(format!("{}", value));
            }
        }
        row_count += 1;
    }

    // Print row and column count
    println!("Rows: {}, Columns: {}", row_count, column_names.len());

    // Print each column summary
    for (name, data) in column_names.iter().zip(column_data.iter()) {
        let display_data = if data.len() == 5 {
            format!("[{}, {}, ..., {}, {}]", data[0], data[1], data[3], data[4])
        } else {
            format!("[{}]", data.join(", "))
        };
        println!("{} {}", name, display_data);
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
                .index(1) // this will always be the first positional argument
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
        .arg(
            Arg::new("columns")
            .short('c')
            .long("columns")
            .help("Prints only the selected columns by name or index")
            .num_args(1..)
            .value_delimiter(',')
        )
        .arg(
            Arg::new("summary")
            .short('s')
            .long("summary")
            .help("Prints a summary of the Parquet file")
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
    } else if let Some(columns) = matches.get_many::<String>("columns") {
        let columns: Vec<String> = columns.map(|s| s.to_string()).collect();
        print_selected_columns(&reader, columns);
    } else if *matches.get_one::<bool>("summary").unwrap_or(&false) {
        print_summary(&reader);
    } else {
        print_columns_and_data(reader);
    }

    Ok(())
}
