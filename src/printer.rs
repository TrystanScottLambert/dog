// printing module handling all printing functions and routines

use colored::Colorize;
use polars::prelude::*;
use polars::prelude::{Column, CsvWriter};
use std::fs::File;
use std::path::PathBuf;

pub fn print_only_data(lazy_frame: LazyFrame, include_header: bool) {
    let mut out = std::io::stdout().lock();
    let mut df = lazy_frame
        .collect()
        .expect("Error converting to data frame.");

    CsvWriter::new(&mut out)
        .include_header(include_header)
        .with_separator(b' ')
        .finish(&mut df)
        .expect("Failed to write full CSV to stdout");
}

pub fn print_schema(lazy_frame: LazyFrame) {
    let mut mut_lazyframe = lazy_frame;
    let schema = mut_lazyframe
        .collect_schema()
        .expect("Trouble Reading Schema");
    println!("{:#?}", schema)
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

fn create_col_summary_string(column: &Column) -> String {
    let mut output = Vec::new();
    for series in column.as_series().into_iter() {
        for val in series.iter() {
            output.push(format!("{}", val));
        }
    }

    if column.len() == 6 {
        output.insert(3, "…".to_string());
    } else if column.len() > 6 {
        panic!("There should not be more than 6 items in the summary row.")
    }

    format!(
        "{}{}{}{}",
        column.name().blue(),
        ": [".bold(),
        output.join(", "),
        "]".bold()
    )
}

fn get_number_rows(lazy_frame: LazyFrame) -> u32 {
    // Source - https://stackoverflow.com/a/73534468
    // Posted by Niklas Mohrin, modified by community. See post 'Timeline' for change history
    // Retrieved 2026-05-02, License - CC BY-SA 4.0
    lazy_frame
        .select([len().alias("count")])
        .collect()
        .unwrap()
        .column("count")
        .unwrap()
        .u32()
        .unwrap()
        .get(0)
        .unwrap()
}

fn get_number_columns(lazy_frame: LazyFrame) -> u32 {
    let mut mut_lazyframe = lazy_frame;
    mut_lazyframe.collect_schema().unwrap().len() as u32
}

pub fn print_summary(lazy_frame: LazyFrame) {
    let df: DataFrame;
    let number_of_rows = get_number_rows(lazy_frame.clone());
    let number_of_columns = get_number_columns(lazy_frame.clone());
    if number_of_rows < 6 {
        df = lazy_frame.collect().expect("Can't convert to dataframe");
    } else {
        let df_head = lazy_frame
            .clone()
            .slice(0, 3)
            .collect()
            .expect("Couldn't convert head ");
        let df_tail = lazy_frame
            .clone()
            .tail(3)
            .collect()
            .expect("Couldn't convert");
        df = df_head.vstack(&df_tail).unwrap();
    }

    let column_data = df.columns();

    print!(
        "{}{}\n{}{}\n\n",
        "Number of Rows: ".bold(),
        number_of_rows.to_string().green(),
        "Number of columns: ".bold(),
        number_of_columns.to_string().green()
    );

    let summaries = column_data
        .iter()
        .map(create_col_summary_string)
        .collect::<Vec<String>>();
    println!("{}", summaries.join("\n"));
}

pub fn peak(lazy_frame: LazyFrame) {
    // prints out the polars data frame as 'peak'.
    println!("{:?}", lazy_frame.collect().unwrap())
}
