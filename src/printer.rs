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

pub fn check_for_keyword_metadata(file_name: &PathBuf, keyword: &str) -> Result<bool> {
    let file = File::open(file_name)?;
    let mut reader = ParquetReader::new(file);
    if let Some(kv_metadata) = reader.get_metadata()?.key_value_metadata() {
        for kv in kv_metadata {
            if kv.key == keyword {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub fn list_keyword_metadata(file_name: &PathBuf) -> Result<()> {
    let file = File::open(file_name)?;
    let mut reader = ParquetReader::new(file);
    let kv_metadata = reader
        .get_metadata()?
        .key_value_metadata()
        .as_ref()
        .unwrap();
    for key in kv_metadata {
        println!("{}", key.key.bold().magenta());
    }
    Ok(())
}

pub fn print_keyword_metadata(file_name: &PathBuf, keyword: &str) -> Result<()> {
    let file = File::open(file_name)?;
    let mut reader = ParquetReader::new(file);

    // Get the file metadata
    if let Some(kv_metadata) = reader.get_metadata()?.key_value_metadata() {
        // Look for keyword maml
        for kv in kv_metadata {
            if kv.key == keyword {
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

pub fn print_tail(lazy_frame: &LazyFrame, number_of_rows: u32) -> Result<()> {
    let tail = lazy_frame.clone().tail(number_of_rows);
    print_catlike(tail)?;
    Ok(())
}

pub fn print_head(lazy_frame: &mut LazyFrame, number_of_rows: u32) -> Result<()> {
    let head_frame = lazy_frame.clone();
    let head = head_frame.limit(number_of_rows);
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

fn fmt_cell(stats: &DataFrame, col: &str) -> Result<String> {
    Ok(format!("{}", stats.column(col)?.get(0)?))
}

enum ColKind {
    Numeric,
    Str,
    Other,
}

fn classify(dtype: &DataType) -> ColKind {
    if dtype.is_primitive_numeric() {
        ColKind::Numeric
    } else if dtype.is_string() {
        ColKind::Str
    } else {
        ColKind::Other // bool, dates, etc.
    }
}
pub fn print_stats(lazy_frame: LazyFrame) -> Result<()> {
    let mut lf = lazy_frame.clone();
    let schema = lf.collect_schema()?;

    for (name, dtype) in schema.iter() {
        let n = name.as_str();
        let c = col(n);

        // only this column's expressions
        let mut exprs: Vec<Expr> = vec![c.clone().null_count().alias("nulls")];
        match classify(dtype) {
            ColKind::Numeric => {
                exprs.push(c.clone().min().alias("min"));
                exprs.push(c.clone().mean().alias("mean"));
                exprs.push(c.clone().median().alias("median"));
                exprs.push(c.clone().max().alias("max"));
                exprs.push(c.std(1).alias("std"));
            }
            ColKind::Str => {
                exprs.push(c.clone().min().alias("min"));
                exprs.push(c.clone().max().alias("max"));
                exprs.push(c.n_unique().alias("nunique"));
            }
            ColKind::Other => {
                exprs.push(c.clone().min().alias("min"));
                exprs.push(c.max().alias("max"));
            }
        }

        // projection pushdown => scan reads only column `n`
        let stats = lazy_frame
            .clone()
            .select(exprs)
            .collect_with_engine(Engine::Streaming)?
            .unwrap_single();

        println!("{}:", n.bold());
        println!("---------------");
        match classify(dtype) {
            ColKind::Numeric => {
                println!("min: {}", fmt_cell(&stats, "min")?.green());
                println!("mean: {}", fmt_cell(&stats, "mean")?.green());
                println!("median: {}", fmt_cell(&stats, "median")?.green());
                println!("max: {}", fmt_cell(&stats, "max")?.green());
                println!("std: {}", fmt_cell(&stats, "std")?.green());
            }
            ColKind::Str => {
                println!("min: {}", fmt_cell(&stats, "min")?.green());
                println!("max: {}", fmt_cell(&stats, "max")?.green());
                println!("unique: {}", fmt_cell(&stats, "nunique")?.green());
            }
            ColKind::Other => {
                println!("min: {}", fmt_cell(&stats, "min")?.green());
                println!("max: {}", fmt_cell(&stats, "max")?.green());
            }
        }
        println!("null counts: {}", fmt_cell(&stats, "nulls")?.green());
        println!();
        // `stats` drops here; next column starts clean
    }
    Ok(())
}
