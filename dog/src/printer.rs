// printing module handling all printing functions and routines

use std::fs::File;
use polars::{frame::DataFrame, prelude::{Column, CsvWriter}};
use polars::prelude::*;


pub fn print_only_data(data_frame: DataFrame, include_header: bool) {
    let mut out = std::io::stdout().lock();
    let mut df = data_frame.clone();  // Make a mutable copy just for writing
    
    if include_header == true {
        CsvWriter::new(&mut out)
        .include_header(true)
        .with_separator(b' ')
        .finish(&mut df)
        .expect("Failed to write full CSV to stdout");
    } else {
        CsvWriter::new(&mut out)
        .with_separator(b' ')
        .finish(&mut df)
        .expect("Failed to write full CSV to stdout");
    }
}

pub fn print_metadata(file_name: &str) {
    let file = File::open(file_name).unwrap();
    let schema = ParquetReader::new(file).schema().unwrap();
    println!("{:#?}", schema);
}

pub fn print_column_names(data_frame: DataFrame) {
    let col_names = data_frame.get_column_names_str();
    println!("{}", col_names.join("\n"));
}

fn print_catlike(data_frame: DataFrame) {
    // prints the data frame on a row x row basis like cat would.
    let height = data_frame.height();
    let columns = data_frame.get_columns();

    for i in 0..height {
        let row_vals: Vec<String> = columns
            .iter()
            .map(|s| format!("{}", s.get(i).unwrap()))
            .collect();
        println!("{}", row_vals.join(" "));
    }
}

pub fn print_tail(data_frame: DataFrame) {
    let tail = data_frame.tail(Some(10));
    print_catlike(tail);
}

pub fn print_head(data_frame: DataFrame) {
    let head = data_frame.head(Some(10));
    print_column_names(data_frame);
    print_catlike(head);
}

fn print_col_summary(column: &Column) {
    if column.len() > 6 {
        let top_col = column.head(Some(3));
        let bottom_col = column.tail(Some(3));
        let mut output = Vec::new();
    
        for series in top_col.as_series().into_iter() {
            for val in series.iter() {
                output.push(format!("{}", val));
            }
        }
    
        output.push("...".to_string());
        for series in bottom_col.as_series().into_iter() {
            for val in series.iter() {
                output.push(format!("{}", val));
            }
        }
        println!("{}: [{}]",column.name(), output.join(","))
       
    } else {
        let top_col = column.head(Some(column.len()));
        let mut output = Vec::new();

        for series in top_col.as_series().into_iter() {
            for val in series.iter() {
                output.push(format!("{}", val));
            }
        }
        println!("{}: [{}]",column.name(), output.join(","))
    }
}

pub fn print_summary(reader: DataFrame) {
    let column_data= reader.get_columns();
    let (number_of_rows, number_of_columns) = reader.shape();
    
    print!("Number of Rows: {number_of_rows}\nNumber of columns: {number_of_columns} \n\n");
    
    for column in column_data.iter() {
        print_col_summary(column);
    }

}

pub fn peak(data_frame: DataFrame) {
    // prints out the polars data frame as 'peak'.
    println!("{:?}", data_frame)
}
