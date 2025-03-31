mod printer;
mod reader;
mod cli;

use crate::printer::*;
use crate::reader::read_parquet_file;
use std::fs::File;
use clap::ArgMatches;
use parquet::file::reader::SerializedFileReader;



fn handle_arguments(matches: ArgMatches, reader: SerializedFileReader<File>) {
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
}

fn main(){
    let matches = cli::build_cli().get_matches();
    let file = matches.get_one::<String>("file").expect("File argument missing");
    let reader = read_parquet_file(file);
    handle_arguments(matches, reader);
}
