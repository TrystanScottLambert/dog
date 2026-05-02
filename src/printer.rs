// printing module handling all printing functions and routines

use polars::prelude::*;
use polars::prelude::{Column, CsvWriter};
use std::fs::File;
use std::path::PathBuf;

pub fn print_only_data(lazy_frame: LazyFrame, include_header: bool) {
    let mut out = std::io::stdout().lock();
    let mut df = lazy_frame
        .collect()
        .expect("Error converting to data frame.");

    if include_header {
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

pub fn print_metadata(file_name: &PathBuf) {
    let file = File::open(file_name).expect("Problem reading file file.");
    let schema = ParquetReader::new(file)
        .schema()
        .expect("Problem reading header.");
    println!("{:#?}", schema);
}

pub fn print_waves_metadata(file_name: &PathBuf) {
    let file = File::open(file_name).expect("Problem reading file.");
    let mut reader = ParquetReader::new(file);

    // Get the file metadata
    match reader.get_metadata() {
        Ok(file_metadata) => {
            if let Some(kv_metadata) = file_metadata.key_value_metadata() {
                // Look for keyword maml
                for kv in kv_metadata {
                    if kv.key == "maml" {
                        if let Some(value) = &kv.value {
                            println!("{}", value);
                            return;
                        }
                    }
                }
                println!("No MAML metadata found in file.");
            } else {
                println!("No metadata found in file.");
            }
        }
        Err(e) => println!("Error reading metadata: {}", e),
    }
}

pub fn print_column_names(lazy_frame: &mut LazyFrame) {
    let col_names: Vec<String> = lazy_frame
        .collect_schema()
        .expect("Schema couldn't be resolved")
        .iter_names()
        .map(|name| name.to_string())
        .collect();
    println!("{}", col_names.join("\n"));
}

fn print_catlike(lazy_frame: LazyFrame) {
    // prints the data frame on a row x row basis like cat would.
    let df = lazy_frame.collect().expect("Couldn't convert lf to df.");
    let number_of_rows = df.height();
    let columns = df.columns();

    for i in 0..number_of_rows {
        let row_vals: Vec<String> = columns
            .iter()
            .map(|s| format!("{}", s.get(i).unwrap()))
            .collect();
        println!("{}", row_vals.join(" "));
    }
}

pub fn print_tail(lazy_frame: LazyFrame) {
    let tail = lazy_frame.tail(10);
    print_catlike(tail);
}

pub fn print_head(lazy_frame: &mut LazyFrame) {
    let head_frame = lazy_frame.clone();
    let head = head_frame.slice(0, 10);
    print_column_names(lazy_frame);
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
        println!("{}: [{}]", column.name(), output.join(","))
    } else {
        let top_col = column.head(Some(column.len()));
        let mut output = Vec::new();

        for series in top_col.as_series().into_iter() {
            for val in series.iter() {
                output.push(format!("{}", val));
            }
        }
        println!("{}: [{}]", column.name(), output.join(","))
    }
}

pub fn print_summary(lazy_frame: LazyFrame) {
    let df = lazy_frame.collect().expect("Couldn't convert");
    let column_data = df.columns();
    let (number_of_rows, number_of_columns) = df.shape();

    print!("Number of Rows: {number_of_rows}\nNumber of columns: {number_of_columns} \n\n");

    for column in column_data.iter() {
        print_col_summary(column);
    }
}

pub fn peak(lazy_frame: LazyFrame) {
    // prints out the polars data frame as 'peak'.
    println!("{:?}", lazy_frame.collect().unwrap())
}
