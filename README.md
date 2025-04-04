dog. It's like cat, but for parquet.

# Motivation
`dog` is meant to be a clean way of quickly inspecting parquet files in the same way that one might done using `cat`. `cat` is the concatenates text files and prints them to standard output and so it's quite commonly used in terminal environments to check the contents of files such as .csv or .txt.

However, `cat` is not useful for some file formats which do are not simple text files (e.g. .fits or .parquet). 

`dog` aims to be an alternative to cat for these kinds of files. 

# Installation	
Currently `dog` is only available on mac-os but will be made availble on linux soon. 

**Installing dog is very easy**
```
curl -L -o dog https://github.com/trystanscottlambert/dog/releases/download/v0.1.0/dog-macos-x86_64
chmod +x dog
sudo mv /usr/local/bin/
```

You may need to start a new terminal to get it working.
# Usage

The help file for `dog` is available as `dog -h` or `dog --help` but we explain some common usages below. The options for dog can be supplied either before the file name (if there are no parameters that need to be passed) or after the file name.

We use a file name `test_file.parquet` in these examples. 

### Simple case
In the simple case the user need not inlcude any options and just run:

```bash
dog test_file.parquet
```
This will print out the entire data of the parquet file in columnar format including the column names. However sometimes this will be a large list and it might not be possible to see the top of the file.

### Head and tail
Often users will combine `head` and `tail` with `cat` in order to inspect the bottom and top of the file. This is useful when the file is large. For ease of use we include both the `-H --head` and `-t --tail` options which will print the first and last 10 rows of data respectively. 
```bash
dog -H test_file.parquet
dog -t test_file.parquet
```
Tail will need to read the entire file into memory and should be used sparingly. 
It is worth noting that the normal `head` and `tail` tools can be used in conjunction with `dog` by piping the output. So while there is no option for selecting the number of rows from the user this functionality can be mimicked. For example

```bash
dog test_file.parquet | head -n 20
```
will print the first 20 rows of the output from `dog` (19 rows of data plus the header)



### Column names
Often times it is useful to get the full names of columns in a file. This can be done with `-n --names`
```bash
dog -n test_file.parquet
```

### Printing only selected columns
It's possible to only select certain columns. For example, we might have columns such as `gal_id_new`, `gal_id_old`, `ra`, `dec`, `z_obs`, `z_cos`, in that order. In some cases we may only be interested in one of these columns, or a subset. 

```bash
dog test_file.parquet -c gal_id_new,ra,dec,z_obs
```

will print out only those columns. The order remains the same so
```bash
dog test_file.parquet -c ra,z_obs,dec,gal_id_new
```
will result in the exact same output. Alternatively we can use the column index. 

both 
```bash
dog test_file.parquet -c 0,2,4
dog test_file.parquet -c gal_id_new,ra,z_obs
```
are equivalent. **Note that `dog` is 0 indexed meaning the first column is 0.**

the `-c --columns` option must follow **after** the file name. i.e, `dog -c 0 test_file.parquet` will not work.

### Summary
A summary of the entire contents is availble with the `-s --summary` option. 

```bash
dog -s test_file.parquet
```

will produce the number of rows and columns of the table and the first and last couple of data points for each column. 
```
Rows: 484551, Columns: 3
id_galaxy_sky [68196, 68198, ..., 68202, 68204]
type [0, 0, ..., 0, 0]
log_mstar_total [10.768383, 10.1552515, ..., 9.557503, 9.438515]
```
