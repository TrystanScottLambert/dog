mod cli;
mod filter;
mod maml_footer;
mod printer;
mod reader;
mod write;

use std::path::PathBuf;

use crate::filter::parse_selection_string;
use crate::maml_footer::write_waves_metadata;
use crate::printer::*;
use crate::reader::{read_file, read_yaml, which_file, FileType};
use crate::write::write_parquet;
use anyhow::Result;
use clap::ArgMatches;
use polars::prelude::*;

fn handle_arguments(matches: ArgMatches) -> Result<()> {
    let file = matches
        .get_one::<String>("file")
        .expect("File argument missing");
    let file_path = PathBuf::from(file);

    if let Some(maml_file) = matches.get_one::<String>("insert-maml") {
        let maml = read_yaml(PathBuf::from(maml_file))?;
        let force = matches.get_flag("force");
        if !force && check_for_maml_metadata(&file_path)? {
            anyhow::bail!(
                "{} already contains MAML metadata; pass -F to overwrite; run `dog -w {}` to view.",
                file_path.display(),
                file_path.display()
            );
        }
        write_waves_metadata(&file_path, &maml)?;
        return Ok(());
    }

    let mut lazy_frame = read_file(file_path.clone())?;
    let mut columns_selected = false;
    let mut rows_selected = false;

    // Optional column filtering BEFORE any printing
    if let Some(columns) = matches.get_many::<String>("columns") {
        let columns: Vec<Expr> = columns.map(col).collect();
        lazy_frame = lazy_frame.select(columns);
        columns_selected = true;
    }

    if let Some(filter_selection) = matches.get_one::<String>("filter") {
        let polars_expresion = match parse_selection_string(filter_selection) {
            Ok(expr) => expr,
            _ => anyhow::bail!(
                "Error parsing the filter selection string: {}",
                filter_selection
            ),
        };
        lazy_frame = lazy_frame.filter(polars_expresion);
        rows_selected = true;
    }

    if let Some(outfile_name) = matches.get_one::<String>("outfile") {
        if rows_selected | columns_selected {
            write_parquet(&lazy_frame, &PathBuf::from(outfile_name))?;

            return Ok(());
        } else {
            anyhow::bail!("File not saved. No columns or rows have been selected.")
        }
    }

    if *matches.get_one::<bool>("names").unwrap_or(&false) {
        print_column_names(&mut lazy_frame)?;
    } else if *matches.get_one::<bool>("data").unwrap_or(&false) {
        print_only_data(lazy_frame, false)?;
    } else if *matches.get_one::<bool>("tail").unwrap_or(&false) {
        print_tail(lazy_frame)?;
    } else if *matches.get_one::<bool>("head").unwrap_or(&false) {
        print_head(&mut lazy_frame)?;
    } else if *matches.get_one::<bool>("stats").unwrap_or(&false) {
        print_stats(lazy_frame)?;
    } else if *matches.get_one::<bool>("schema").unwrap_or(&false) {
        print_schema(lazy_frame)?;
    } else if *matches.get_one::<bool>("maml").unwrap_or(&false) {
        print_waves_metadata(&file_path)?;
    } else if *matches.get_one::<bool>("summary").unwrap_or(&false) {
        print_summary(lazy_frame)?;
    } else if *matches.get_one::<bool>("peak").unwrap_or(&false) {
        peak(lazy_frame)?;
    } else if *matches.get_one::<bool>("convert").unwrap_or(&false) {
        let outfile = match which_file(&file_path)? {
            FileType::Csv => PathBuf::from(file.replace(".csv", "_converted.parquet")),
            FileType::Fits => PathBuf::from(file.replace(".fits", "_converted.parquet")),
            FileType::Parquet => panic!("File is already a parquet!"),
        };
        write_parquet(&lazy_frame, &outfile).unwrap();
    } else {
        print_only_data(lazy_frame, true)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let matches = cli::build_cli().get_matches();
    handle_arguments(matches)?;
    Ok(())
}
