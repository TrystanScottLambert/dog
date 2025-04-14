mod printer;
mod reader;
mod cli;


use crate::printer::*;
use polars::prelude::*;
use crate::reader::read_parquet_file;
use crate::reader::read_parquet_file_polars;
use crate::printer::print_summary_polars;
use std::fs::File;
use clap::ArgMatches;
use parquet::file::reader::SerializedFileReader;



fn handle_arguments(matches: ArgMatches, reader: SerializedFileReader<File>, data_frame: DataFrame) {
    if *matches.get_one::<bool>("names").unwrap_or(&false) {
        print_column_names(data_frame);
    } else if *matches.get_one::<bool>("data").unwrap_or(&false) {
        print_only_data(&reader).unwrap();
    } else if *matches.get_one::<bool>("tail").unwrap_or(&false) {
        print_tail_polars(data_frame);
    } else if *matches.get_one::<bool>("head").unwrap_or(&false) {
        print_head_polars(data_frame);
    } else if *matches.get_one::<bool>("META").unwrap_or(&false) {
        print_metadata(&reader);
    } else if let Some(columns) = matches.get_many::<String>("columns") {
        let columns: Vec<String> = columns.map(|s| s.to_string()).collect();
        print_selected_columns(&reader, columns);
    } else if *matches.get_one::<bool>("summary").unwrap_or(&false) {
        print_summary_polars(data_frame);
    } else if *matches.get_one::<bool>("peak").unwrap_or(&false) {
        peak(data_frame);
    } else {
        print_columns_and_data(reader, data_frame);
    }
}


fn main(){
    let matches = cli::build_cli().get_matches();
    let file = matches.get_one::<String>("file").expect("File argument missing");
    let reader = read_parquet_file(file);
    let data_frame = read_parquet_file_polars(file);
    handle_arguments(matches, reader, data_frame);

    //let file_name = "../../waves_wide_missingcols_bp0p1p0.parquet";
    //let file_name = "/Users/00115372/Desktop/mock_catalogs/offical_waves_mocks/waves_deep_gals.parquet";
    //let mut file = std::fs::File::open(file_name).unwrap();
    //let df = read_parquet_file_polars(file_name);
    //let reader = read_parquet_file(file_name);

    //let columns = vec!["ra".to_string(), "dec".to_string(), "zobs".to_string()];
    //let now = Instant::now();
    //print_selected_columns(&reader, columns);
    //let arrow_elapsed = now.elapsed();
    //let columns = vec!["ra".to_string(), "dec".to_string(), "zobs".to_string()];
    //let now = Instant::now();
    //print_selected_columns_fast(&reader, columns).unwrap();
    //let polars_elapsed = now.elapsed();
    //println!("Arrow {:?}", arrow_elapsed);
    //println!("Polars {:?}", polars_elapsed);

    //let colnames = df.get_column_names_str();

    //let column = df.column("ra").unwrap();
    //let columns = df.get_columns();
    //let small_column = column.head(Some(20));
    //for name in colnames.iter() {
       // println!("{name}");
    //}
    //let head = column.head(Some(3));


    //let reader = read_parquet_file(file_name);


    //let now = Instant::now();
    //print_summary_polars(df);
    //let elapsed = now.elapsed();
    //println!("{:.2?}", elapsed);
    
    //for series in head.as_series().into_iter() {
    //    for val in series.iter() {
    //        println!("{}", val);
    //    }    
    //}


}