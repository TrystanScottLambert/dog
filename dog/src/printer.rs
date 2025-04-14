// printing module handling all printing functions and routines

use std::{fs::File, process::exit};
use parquet::file::reader::{FileReader, SerializedFileReader};
use polars::{frame::DataFrame, prelude::Column};
use std::io::{self, Write};


pub fn print_only_data(reader: &SerializedFileReader<File>) -> io::Result<()> {
    let mut row_iter = reader.get_row_iter(None).unwrap();
    let stdout = io::stdout(); // lock stdout once
    let mut handle = stdout.lock(); // locking is key for performance

    while let Some(row) = row_iter.next() {
        let row = row.unwrap();
        let mut first = true;
        for (_, value) in row.get_column_iter() {
            if !first {
                write!(handle, " ")?; // write space without newline
            }
            write!(handle, "{}", value)?;
            first = false;
        }
        writeln!(handle)?; // newline after each row
    }
    Ok(())
}


pub fn print_metadata(reader: &SerializedFileReader<File>) {
    let metadata = reader.metadata();
    println!("{:?}", metadata);
}

pub fn print_column_names(data_frame: DataFrame) {
    let col_names = data_frame.get_column_names_str();
    println!("{}", col_names.join(" "));
}

pub fn print_columns_and_data(reader: SerializedFileReader<File>, data_frame: DataFrame) {
    print_column_names(data_frame);
    print_only_data(&reader).unwrap();
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

pub fn print_tail_polars(data_frame: DataFrame) {
    let tail = data_frame.tail(Some(10));
    print_catlike(tail);
}

pub fn print_head_polars(data_frame: DataFrame) {
    let head = data_frame.head(Some(10));
    print_column_names(data_frame);
    print_catlike(head);
}

pub fn print_selected_columns(reader: &SerializedFileReader<File>, columns: Vec<String>) {
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

pub fn print_summary_polars(reader: DataFrame) {
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
