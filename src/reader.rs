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
    let results: Vec<Result<Vec<Column>, Box<dyn std::error::Error + Send + Sync>>> = col_info
        .par_iter()
        .map(
            |(_, col_name, col_type)| -> Result<Vec<Column>, Box<dyn std::error::Error + Send + Sync>> {
                // Each thread needs its own file handle
                let mut local_fptr = FitsFile::open(path)?;
                let local_hdu = local_fptr.hdu(1)?;
                
                // Parse the column type to check for vector columns
                let (repeat_count, type_char) = parse_tform(col_type)?;
                
                let columns = if repeat_count > 1 {
                    // Vector column - read flat data and reshape
                    match type_char {
                        'E' => {
                            let flat_data: Vec<f32> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            expand_vector_column_from_flat::<Float32Type>(col_name, flat_data, repeat_count)?
                        }
                        'D' => {
                            let flat_data: Vec<f64> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            expand_vector_column_from_flat::<Float64Type>(col_name, flat_data, repeat_count)?
                        }
                        'J' => {
                            let flat_data: Vec<i32> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            expand_vector_column_from_flat::<Int32Type>(col_name, flat_data, repeat_count)?
                        }
                        'K' => {
                            let flat_data: Vec<i64> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            expand_vector_column_from_flat::<Int64Type>(col_name, flat_data, repeat_count)?
                        }
                        _ => {
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Unsupported vector column type: {}", col_type),
                            )));
                        }
                    }
                } else {
                    // Scalar column - single column
                    let col = match type_char {
                        'E' => {
                            let data: Vec<f32> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            let ca = Float32Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'D' => {
                            let data: Vec<f64> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            let ca = Float64Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'J' => {
                            let data: Vec<i32> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            let ca = Int32Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'K' => {
                            let data: Vec<i64> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            let ca = Int64Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'A' => {
                            let data: Vec<String> = local_hdu.read_col(&mut local_fptr, col_name)?;
                            let ca = StringChunked::from_iter_values(col_name.into(), data.iter().map(|s| s.as_str()));
                            ca.into_series().into()
                        }
                        _ => {
                            return Err(Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("Unsupported column type: {}", col_type),
                            )));
                        }
                    };
                    vec![col]
                };
                
                Ok(columns)
            },
        )
        .collect();
    
    // Flatten the results and collect all columns
    let mut all_columns = Vec::new();
    for result in results {
        let cols = result.unwrap();
        all_columns.extend(cols);
    }
    
    let df = DataFrame::new(all_columns)?;
    Ok(df)
}

// Parse TFORM string like "101E" into (repeat_count=101, type_char='E')
fn parse_tform(tform: &str) -> Result<(usize, char), Box<dyn std::error::Error + Send + Sync>> {
    let type_char = tform.chars().last()
        .ok_or_else(|| std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Empty TFORM string"
        ))?;
    
    let repeat_str: String = tform.chars()
        .take_while(|c| c.is_numeric())
        .collect();
    
    let repeat_count = if repeat_str.is_empty() {
        1
    } else {
        repeat_str.parse::<usize>()?
    };
    
    Ok((repeat_count, type_char))
}

// Expand a vector column from flat data (FITS stores vectors as flattened arrays)
fn expand_vector_column_from_flat<T: PolarsNumericType>(
    base_name: &str,
    flat_data: Vec<T::Native>,
    n_elements: usize,
) -> Result<Vec<Column>, Box<dyn std::error::Error + Send + Sync>>
where
    T::Native: Send + Sync + Clone,
    ChunkedArray<T>: IntoSeries,
{
    let n_rows = flat_data.len() / n_elements;
    let mut columns = Vec::new();
    
    for i in 0..n_elements {
        // Extract every n_elements-th value starting at offset i
        let col_data: Vec<T::Native> = (0..n_rows)
            .map(|row| flat_data[row * n_elements + i])
            .collect();
        
        let col_name = format!("{}_{}", base_name, i);
        let ca = ChunkedArray::<T>::from_vec(col_name.into(), col_data);
        columns.push(ca.into_series().into());
    }
    
    Ok(columns)
}



pub fn read_file(file_name: &str) -> DataFrame {
    match which_file(file_name) {
        FileType::Csv => read_csv_file(file_name),
        FileType::Parquet => read_parquet_file(file_name),
        FileType::Fits => read_fits_file(file_name).unwrap(),
    }
}
