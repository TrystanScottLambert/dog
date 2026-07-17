mod cli;
mod filter;
mod footer;
mod printer;
mod reader;
mod write;

use std::path::PathBuf;

use crate::filter::parse_selection_string;
use crate::footer::{delete_keyword_metadata, write_keyword_metadata};
use crate::printer::*;
use crate::reader::{read_file, which_file, FileType};
use crate::write::write_parquet;
use anyhow::{bail, Result};
use clap::ArgMatches;
use polars::prelude::*;

fn handle_arguments(matches: ArgMatches) -> Result<()> {
    let files = matches
        .get_many::<String>("file")
        .expect("File argument missing");

    for file in files {
        let file_path = PathBuf::from(file);
        if !file_path.exists() {
            bail!("No file follows glob pattern '{}'", file_path.display());
        }

        if let Some(meta_args) = matches.get_many::<String>("insert-metadata") {
            let mut arguments = meta_args.map(|f| f.to_string());
            let meta_file = PathBuf::from(arguments.next().unwrap());
            let keyword = arguments.next().unwrap();
            if !meta_file.exists() {
                bail!("meta file '{}' does not exist", meta_file.display());
            }

            let maml = std::fs::read_to_string(meta_file)?;
            let force = matches.get_flag("force");
            if !force && check_for_keyword_metadata(&file_path, &keyword)? {
                bail!(
                "{} already contains '{}' keyword-metadata; pass -F to overwrite; run `dog -w {} {}` to view.",
                file_path.display(),
                keyword,
                keyword,
                file_path.display()
            );
            }
            write_keyword_metadata(&file_path, &maml, &keyword)?;
            continue;
        }

        if let Some(keyword) = matches.get_one::<String>("delete-kw-metadata") {
            if !check_for_keyword_metadata(&file_path, keyword)? {
                eprintln!(
                    "File name '{}' does not have a keyword '{}' in it's metadata. run `dog --list-keywords {}` to list current keywords",
                    file_path.display(),
                    keyword,
                    file_path.display(),
                )
            } else {
                delete_keyword_metadata(&file_path, keyword)?;
            }
            continue;
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
                _ => bail!(
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
                continue;
            } else {
                bail!("File not saved. No columns or rows have been selected.")
            }
        }

        if let Some(header_rows) = matches.get_one::<String>("head") {
            let no_rows: u32 = match header_rows.trim().parse() {
                Ok(no_row) => no_row,
                Err(_) => bail!("'Number of rows' should be an integer."),
            };
            print_head(&mut lazy_frame, no_rows)?;
        }

        if let Some(tail_rows) = matches.get_one::<String>("tail") {
            let no_rows: u32 = match tail_rows.trim().parse() {
                Ok(no_row) => no_row,
                Err(_) => bail!("'Number of rows' should be an integer."),
            };
            print_tail(&lazy_frame, no_rows)?;
        }

        if let Some(keyword) = matches.get_one::<String>("keyword") {
            print_keyword_metadata(&file_path, keyword)?;
            continue;
        }

        if matches.get_flag("names") {
            print_column_names(&mut lazy_frame)?;
        } else if matches.get_flag("data") {
            print_only_data(lazy_frame, false)?;
        } else if matches.get_flag("stats") {
            print_stats(lazy_frame)?;
        } else if matches.get_flag("schema") {
            print_schema(lazy_frame)?;
        } else if matches.get_flag("summary") {
            print_summary(lazy_frame)?;
        } else if matches.get_flag("peak") {
            peak(lazy_frame)?;
        } else if matches.get_flag("list-kw-metadata") {
            list_keyword_metadata(&file_path)?;
        } else if matches.get_flag("convert") {
            let outfile = match which_file(&file_path)? {
                FileType::Csv => PathBuf::from(file.replace(".csv", "_converted.parquet")),
                FileType::Fits => PathBuf::from(file.replace(".fits", "_converted.parquet")),
                FileType::Parquet => panic!("File is already a parquet!"),
            };
            write_parquet(&lazy_frame, &outfile).unwrap();
        } else {
            print_only_data(lazy_frame, true)?;
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let matches = cli::build_cli().get_matches();
    handle_arguments(matches)?;
    Ok(())
}
