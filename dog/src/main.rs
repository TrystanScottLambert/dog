mod printer;
mod reader;
mod cli;

use crate::printer::*;
use crate::reader::read_parquet_file_polars;
use crate::printer::print_summary_polars;
use clap::ArgMatches;


fn handle_arguments(matches: ArgMatches) {
    let file = matches.get_one::<String>("file").expect("File argument missing");
    let data_frame = read_parquet_file_polars(file);
    
    if *matches.get_one::<bool>("names").unwrap_or(&false) {
        print_column_names(data_frame);
    } else if *matches.get_one::<bool>("data").unwrap_or(&false) {
        print_only_data_polars(data_frame, false);
    } else if *matches.get_one::<bool>("tail").unwrap_or(&false) {
        print_tail_polars(data_frame);
    } else if *matches.get_one::<bool>("head").unwrap_or(&false) {
        print_head_polars(data_frame);
    } else if *matches.get_one::<bool>("META").unwrap_or(&false) {
        print_metadata(file);
    } else if let Some(columns) = matches.get_many::<String>("columns") {
        let columns: Vec<String> = columns.map(|s| s.to_string()).collect();
        print_selected_columns_polars(data_frame, columns);
    } else if *matches.get_one::<bool>("summary").unwrap_or(&false) {
        print_summary_polars(data_frame);
    } else if *matches.get_one::<bool>("peak").unwrap_or(&false) {
        peak(data_frame);
    } else {
        print_only_data_polars(data_frame, true);
    }
}


fn main(){
    let matches = cli::build_cli().get_matches();
    handle_arguments(matches);
}
