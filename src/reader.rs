use fitsio_pure::compat::fitsfile::FitsFile;
use polars::prelude::*;
use polars::{frame::DataFrame};
use std::path::{PathBuf, Path};
use rayon::prelude::*;
use anyhow::{Result, Context, anyhow};


pub enum FileType {
    Fits,
    Csv,
    Parquet,
}

pub fn which_file(file_name: &Path) -> Result<FileType> {
    let extension = match file_name.extension() {
        Some(t) => t.to_str().context("Failed to convert OS string to str"),
        None => Err(anyhow!("{file_name:?} has no extension. Don't know how to read it."))
    };
    match extension? {
        "parquet" => Ok(FileType::Parquet),
        "csv" => Ok(FileType::Csv),
        "fits" => Ok(FileType::Fits),
        _ => Err(anyhow!("{file_name:?} has an unsupported extension")),
    }
}

pub fn read_parquet_file(file_name: PathBuf)-> Result<LazyFrame> {
    Ok(LazyFrame::scan_parquet_files(vec![PlRefPath::new(file_name.to_str().expect("Path {file_name:?} is not utf8"))].into(), ScanArgsParquet::default())?)
}

pub fn read_csv_file(path: PathBuf) -> Result<LazyFrame> {
    let lf = LazyCsvReader::new(PlRefPath::new(path.to_str().expect("Path {file_name:?} is not utf8"))).finish()?;
    Ok(lf)
}


pub fn read_fits_file(path: &PathBuf) -> Result<LazyFrame> {
    let fptr = FitsFile::open(path)?;
    let hdu = fptr.hdu(1)?;
    let num_cols: i64 = hdu.read_key(&fptr, "TFIELDS")?;

    // Collect column metadata first
    let mut col_info = Vec::new();
    for i in 1..=num_cols {
        let col_name: String = hdu.read_key(&fptr, &format!("TTYPE{}", i))?;
        let col_type: String = hdu.read_key(&fptr, &format!("TFORM{}", i))?;
        col_info.push((i, col_name, col_type));
    }
    
    // Read columns in parallel
    let results: Vec<Result<Vec<Column>, Box<dyn std::error::Error + Send + Sync>>> = col_info
        .par_iter()
        .map(
            |(_, col_name, col_type)| -> Result<Vec<Column>, Box<dyn std::error::Error + Send + Sync>> {
                // Each thread needs its own file handle
                let local_fptr = FitsFile::open(path)?;
                let local_hdu = local_fptr.hdu(1)?;
                
                // Parse the column type to check for vector columns
                let (repeat_count, type_char) = parse_tform(col_type)?;
                
                let columns = if repeat_count > 1 && type_char != 'A' {
                    // Vector column - read flat data and reshape
                    match type_char {
                        'E' => {
                            let flat_data: Vec<f32> = local_hdu.read_col(&local_fptr, col_name)?;
                            expand_vector_column_from_flat::<Float32Type>(col_name, flat_data, repeat_count)?
                        }
                        'D' => {
                            let flat_data: Vec<f64> = local_hdu.read_col(&local_fptr, col_name)?;
                            expand_vector_column_from_flat::<Float64Type>(col_name, flat_data, repeat_count)?
                        }
                        'J' => {
                            let flat_data: Vec<i32> = local_hdu.read_col(&local_fptr, col_name)?;
                            expand_vector_column_from_flat::<Int32Type>(col_name, flat_data, repeat_count)?
                        }
                        'K' => {
                            let flat_data: Vec<i64> = local_hdu.read_col(&local_fptr, col_name)?;
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
                            let data: Vec<f32> = local_hdu.read_col(&local_fptr, col_name)?;
                            let ca = Float32Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'D' => {
                            let data: Vec<f64> = local_hdu.read_col(&local_fptr, col_name)?;
                            let ca = Float64Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'J' => {
                            let data: Vec<i32> = local_hdu.read_col(&local_fptr, col_name)?;
                            let ca = Int32Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'K' => {
                            let data: Vec<i64> = local_hdu.read_col(&local_fptr, col_name)?;
                            let ca = Int64Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'A' => {
                            let data: Vec<String> = local_hdu.read_col(&local_fptr, col_name)?;
                            let ca = StringChunked::from_iter_values(col_name.into(), data.iter().map(|s| s.as_str()));
                            ca.into_series().into()
                        }
                        'I' => {
                            let data: Vec<i32> = local_hdu.read_col(&local_fptr, col_name)?;
                            let ca = Int32Chunked::from_vec(col_name.into(), data);
                            ca.into_series().into()
                        }
                        'L' => {
                            let data: Vec<bool> = local_hdu.read_col(&local_fptr, col_name)?;
                            let ca = BooleanChunked::from_iter_values(col_name.into(), data.into_iter());
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
        let cols = result.map_err(|e| anyhow!("{e}"))?;
        all_columns.extend(cols);
    }
    
    let df = DataFrame::new(all_columns.first().expect("No columns").len(), all_columns)?;
    Ok(df.lazy())
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



pub fn read_file(file_name: PathBuf) -> Result<LazyFrame> {
    match which_file(&file_name)? {
        FileType::Csv => Ok(read_csv_file(file_name)?),
        FileType::Parquet => Ok(read_parquet_file(file_name)?),
        FileType::Fits => Ok(read_fits_file(&file_name)?),
    }
}

