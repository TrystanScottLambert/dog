// printing module handling all printing functions and routines
use anyhow::{Ok, Result};
use colored::Colorize;
use polars::prelude::*;
use polars::prelude::{Column, CsvWriter};
use std::fs::File;
use std::path::PathBuf;

pub fn print_only_data(lazy_frame: LazyFrame, include_header: bool) -> Result<()> {
    let mut out = std::io::stdout().lock();
    let mut df = lazy_frame.collect()?;

    CsvWriter::new(&mut out)
        .include_header(include_header)
        .with_separator(b' ')
        .finish(&mut df)?;
    Ok(())
}

pub fn print_schema(lazy_frame: LazyFrame) -> Result<()> {
    let mut mut_lazyframe = lazy_frame;
    let schema = mut_lazyframe.collect_schema()?;
    println!("{:#?}", schema);
    Ok(())
}

pub fn print_waves_metadata(file_name: &PathBuf) -> Result<()> {
    let file = File::open(file_name)?;
    let mut reader = ParquetReader::new(file);

    // Get the file metadata
    if let Some(kv_metadata) = reader.get_metadata()?.key_value_metadata() {
        // Look for keyword maml
        for kv in kv_metadata {
            if kv.key == "maml" {
                if let Some(value) = &kv.value {
                    println!("{}", value);
                    return Ok(());
                }
            }
        }
    } else {
        println!("No metadata found in file.");
    }
    Ok(())
}

pub fn print_column_names(lazy_frame: &mut LazyFrame) -> Result<()> {
    let col_names: Vec<String> = lazy_frame
        .collect_schema()?
        .iter_names()
        .map(|name| name.to_string())
        .collect();
    println!("{}", col_names.join("\n").green());
    Ok(())
}

fn print_catlike(lazy_frame: LazyFrame) -> Result<()> {
    // prints the data frame on a row x row basis like cat would.
    let df = lazy_frame.collect()?;
    let number_of_rows = df.height();
    let columns = df.columns();

    for i in 0..number_of_rows {
        let row_vals: Vec<String> = columns
            .iter()
            .map(|s| format!("{}", s.get(i).expect("Shouldn't trigger")))
            .collect();
        println!("{}", row_vals.join(" "));
    }
    Ok(())
}

pub fn print_tail(lazy_frame: LazyFrame) -> Result<()> {
    let tail = lazy_frame.tail(10);
    print_catlike(tail)?;
    Ok(())
}

pub fn print_head(lazy_frame: &mut LazyFrame) -> Result<()> {
    let head_frame = lazy_frame.clone();
    let head = head_frame.slice(0, 10);
    print_column_names(lazy_frame)?;
    print_catlike(head)?;
    Ok(())
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

fn get_number_rows(lazy_frame: LazyFrame) -> Result<u32> {
    // Source - https://stackoverflow.com/a/73534468
    // Posted by Niklas Mohrin, modified by community. See post 'Timeline' for change history
    // Retrieved 2026-05-02, License - CC BY-SA 4.0
    Ok(lazy_frame
        .select([len().alias("count")])
        .collect()?
        .column("count")?
        .u32()?
        .get(0)
        .expect("Dataframe appears to be empty."))
}

fn get_number_columns(lazy_frame: LazyFrame) -> Result<u32> {
    let mut mut_lazyframe = lazy_frame;
    Ok(mut_lazyframe.collect_schema()?.len() as u32)
}

pub fn print_summary(lazy_frame: LazyFrame) -> Result<()> {
    let number_of_rows = get_number_rows(lazy_frame.clone())?;
    let number_of_columns = get_number_columns(lazy_frame.clone())?;
    let df = if number_of_rows < 6 {
        lazy_frame.collect()?
    } else {
        let df_head = lazy_frame.clone().slice(0, 3).collect()?;
        let df_tail = lazy_frame.clone().tail(3).collect()?;
        df_head.vstack(&df_tail)?
    };

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
    Ok(())
}

pub fn peak(lazy_frame: LazyFrame) -> Result<()> {
    // prints out the polars data frame as 'peak'.
    println!("{:?}", lazy_frame.collect()?);
    Ok(())
}

pub fn print_stats(lazy_frame: LazyFrame) -> Result<()> {
    let means = lazy_frame
        .clone()
        .select([all().as_expr().mean()])
        .collect()?;
    let medians = lazy_frame.clone().select([all().as_expr().median()]);
    let null_counts = lazy_frame.clone().select([all().as_expr().null_count()]);
    let max_counts = lazy_frame.clone().select([all().as_expr().max()]);
    let min_counts = lazy_frame.clone().select([all().as_expr().min()]);
    for name in lazy_frame.clone().collect_schema()?.iter_names() {
        let col = means.clone();
        println!("{}: {}", name.bold(), col.column(name.as_str())?.get(0)?);
    }
    Ok(())
}
