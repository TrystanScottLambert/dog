// Module which handles reading the parquet file

use fitsio::FitsFile;
use polars::prelude::*;
use polars::{frame::DataFrame, prelude::ParquetReader};
use rayon::prelude::*;
use std::fs::File;

pub enum FileType {
    Fits,
    Csv,
    Parquet,
}

pub fn which_file(file_name: &str) -> FileType {
    let vals = file_name.split(".");
    match vals.last().unwrap() {
        "parquet" => FileType::Parquet,
        "csv" => FileType::Csv,
        "fits" => FileType::Fits,
        _ => FileType::Parquet,
    }
}

pub fn read_parquet_file(file_name: &str) -> DataFrame {
    let mut file = File::open(file_name).expect("Failed to open file");
    ParquetReader::new(&mut file)
        .finish()
        .expect("Failed to parse parquet.")
}

pub fn read_csv_file(path: &str) -> DataFrame {
    let mut file = File::open(path).expect("Can't find message.");
    CsvReader::new(&mut file).finish().expect("Failed parsing.")
}

pub fn read_fits_file(path: &str) -> Result<DataFrame, Box<dyn std::error::Error>> {
    let mut fptr = FitsFile::open(path)?;
    let hdu = fptr.hdu(1)?;
    let num_cols: i64 = hdu.read_key(&mut fptr, "TFIELDS")?;

    // Collect column metadata first
    let mut col_info = Vec::new();
    for i in 1..=num_cols {
        let col_name: String = hdu.read_key(&mut fptr, &format!("TTYPE{}", i))?;
        let col_type: String = hdu.read_key(&mut fptr, &format!("TFORM{}", i))?;
        col_info.push((i, col_name, col_type));
    }

    // Read columns in parallel
    let results: Vec<Result<Column, Box<dyn std::error::Error + Send + Sync>>> = col_info
        .par_iter()
        .map(
            |(_, col_name, col_type)| -> Result<Column, Box<dyn std::error::Error + Send + Sync>> {
                // Each thread needs its own file handle
                let mut local_fptr = FitsFile::open(path)?;
                let local_hdu = local_fptr.hdu(1)?;

                let series = match col_type.chars().last() {
                    Some('E') => {
                        let data: Vec<f32> = local_hdu.read_col(&mut local_fptr, col_name)?;
                        Series::new(col_name.into(), data)
                    }
                    Some('D') => {
                        let data: Vec<f64> = local_hdu.read_col(&mut local_fptr, col_name)?;
                        Series::new(col_name.into(), data)
                    }
                    Some('J') => {
                        let data: Vec<i32> = local_hdu.read_col(&mut local_fptr, col_name)?;
                        Series::new(col_name.into(), data)
                    }
                    Some('K') => {
                        let data: Vec<i64> = local_hdu.read_col(&mut local_fptr, col_name)?;
                        Series::new(col_name.into(), data)
                    }
                    Some('A') => {
                        let data: Vec<String> = local_hdu.read_col(&mut local_fptr, col_name)?;
                        Series::new(col_name.into(), data)
                    }
                    _ => {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Unsupported column type: {}", col_type),
                        ))
                            as Box<dyn std::error::Error + Send + Sync>);
                    }
                };

                Ok(series.into())
            },
        )
        .collect();

    // Convert results to columns, propagating any errors
    let columns: Result<Vec<Column>, _> = results.into_iter().collect();

    let df = DataFrame::new(columns.unwrap())?;
    Ok(df)
}

pub fn read_file(file_name: &str) -> DataFrame {
    match which_file(file_name) {
        FileType::Csv => read_csv_file(file_name),
        FileType::Parquet => read_parquet_file(file_name),
        FileType::Fits => read_fits_file(file_name).unwrap(),
    }
}
