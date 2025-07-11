dog. It's like cat, but for parquet.

# Motivation
Parquet is a relatively new, open source, file format from Apache which is becoming very popular and is already being adopted extensively within data intensive fields. It is a column-orientated format of storing data and benefits from a large amount of compression ([more information is available at the official apache parquet site](https://parquet.apache.org/)). 

Although parquet is quickly being adopted, exploring these files often requires a opening another program (e.g. topcat) or programming language (`R` or `python`) to even take a quick look. Writing three or four lines of code just to see what is in a file is just too many.

`dog` is meant to be a clean way of quickly inspecting .parquet files in the terminal, in same way that might done using `cat`. `cat` concatenates text files and prints them to standard output; It's quite commonly used in terminal environments to check the contents of files such as .csv or .txt.

However, `cat` is not useful for some file formats which are not simple text files (e.g. .fits or .parquet). 

`dog` aims to be an alternative to cat for these kinds of files. 

# Installation	

## Downloading the binary
The binary for `dog` is only available on mac-os but will be made availble on linux soon. 

**Installing dog is very easy**
```
curl -L -o dog https://github.com/trystanscottlambert/dog/releases/download/v0.2.1/dog-macos-x86_64
chmod +x dog
sudo mv dog /usr/local/bin/
```

You may need to start a new terminal to get it working.

If you don't want to install the binary then you can compile the program from source using 'cargo'. 

## Compile from source

If you are using linux or don't want to download a binary file then `dog` can be built from scource using cargo. 

First make sure you have rust installed:
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Download the dog repo with git
```
git clone git@github.com:TrystanScottLambert/dog.git
```

cd into the dog folder
`cd dog/`
You should be able to see the `Cargo.toml` file. From here compile using cargo (which would already be installed with rust.)
```
cargo build --release
```

Then simply move the binary file into your /bin directory

```
sudo mv target/release/dog /usr/local/bin
```

You may need to restart the terminal. 

# Usage

The help file for `dog` is available as `dog -h` or `dog --help` but we explain some common usages below. The options for dog can be supplied either before the file name (if there are no parameters that need to be passed) or after the file name.

We use a file name `test_file.parquet` in these examples. 

### Simple case
In the simple case the user need not include any options and just run:

```bash
dog test_file.parquet
```
This will print out the entire data of the parquet file in a columnar format, including the column names. If the user doesn't want to include the header then the `-d --data` option is available which only prints the data. This might make piping into other functions (like `awk`) slightly easier.

### Head and tail
Often users will combine `head` and `tail` with `cat` in order to inspect the bottom and top of the file. This is useful when the file is large. For ease of use we include both the `-H --head` and `-t --tail` options which will print the first and last 10 rows of data respectively. 
```bash
dog -H test_file.parquet
dog -t test_file.parquet
```
It is worth noting that the normal `head` and `tail` tools can be used in conjunction with `dog` by piping the output. So while there is no option for selecting the number of rows from the user this functionality can be mimicked. For example

```bash
dog test_file.parquet | head -n 20
```
will print the first 20 rows of the output from `dog` (19 rows of data plus the header).



### Column names
Often times it is useful to get the full names of the columns in a file. This can be done with `-n --names`
```bash
dog -n test_file.parquet
```

### Printing only selected columns
It's possible to only select certain columns. For example, we might have columns such as `gal_id_new`, `gal_id_old`, `ra`, `dec`, `z_obs`, `z_cos`, in that order. In some cases we may only be interested in one of these columns, or a subset. 

```bash
dog  -c gal_id_new,ra,dec,z_obs test_file.parquet
```

will print out only those columns. The order remains the same so
```bash
dog  -c ra,z_obs,dec,gal_id_new test_file.parquet
```
will result in the exact same output.

the `-c --columns` option can be used *in combination* with all the other options. If we only want the tail of the ra and dec columns of the parquet file this can be done 
```
dog -c ra,dec -t test_file.parquet
```

This is more useful when combined with the "summary" and "peak" options.

### Summary
A summary of the entire contents is availble with the `-s --summary` option. 

```bash
dog -s test_file.parquet
```

will produce the number of rows and columns of the table and the first and last couple of data points for each column. 
```
Rows: 484551
Columns: 3

id_galaxy_sky [68196, 68198, ..., 68202, 68204]
type [0, 0, ..., 0, 0]
log_mstar_total [10.768383, 10.1552515, ..., 9.557503, 9.438515]
```

### Peak
Another summary view is the `-p --peak` option. This will also give the rows and columns but also include a nicely formated printed out table in polars format.
```
dog -p test_file.parquet
```
However, more often than not, columns and rows will be emitted except for a couple at the corners.

### Metadata
The metadata of the dataframe can also be printed, but in this case this is only the schema which might be incomplete. Future releases will have better meta data handling.
```
dog -M test_file.parquet
```
