`dog`. It's like cat, but for parquet.

![Demo](.github/dog.gif)

# Quick reference

| Option | Arguments | Description |
| --- | --- | --- |
| *(none)* | | Prints the entire table with column names. |
| `-d` `--data` | | Prints only the data, without the header. |
| `-n` `--names` | | Prints only the column names. |
| `-H` `--head` | `<N>` | Prints the top `<N>` rows of data and the column names. |
| `-t` `--tail` | `<N>` | Prints the bottom `<N>` rows of data. |
| `-c` `--columns` | `<COLUMN>` | Prints only the selected columns. Comma separated. |
| `-f` `--filter` | `<SQL-STATEMENT>` | Selects rows with an sql-like statement. E.g. `'ra<10'`. |
| `-o` `--outfile` | `<OUTFILE>` | Saves the current selection to `<OUTFILE>`. Requires `-c` or `-f`. |
| `-s` `--summary` | | Prints the number of rows and columns and the first and last few values of each column. |
| `-p` `--peak` | | Prints a small table in polars format. |
| `--stats` | | Summary statistics for each column, depending on datatype. |
| `--schema` | | Prints the metadata schema. |
| `-k` `--keyword` | `<KEYWORD>` | Prints the `<KEYWORD>` metadata if it exists. |
| `--list-keywords` | | Lists all keyword metadata in the file. |
| `--insert-metadata` | `<METADATA-FILE> <KEYWORD>` | Inserts the contents of `<METADATA-FILE>` at the `<KEYWORD>` position. |
| `--delete-keyword` | `<KEYWORD>` | Deletes the `<KEYWORD>` metadata if it exists. |
| `-F` `--force` | | Overwrites existing keyword metadata. Only with `--insert-metadata`. |
| `--convert` | | Converts a .csv or .fits file into a parquet. |

All options take one or more files, so globs work: `dog -k maml *.parquet`.

# Motivation
Parquet is a relatively new, open source, file format from Apache which is becoming very popular and is already being adopted extensively within data intensive fields. It is a column-orientated format of storing data and benefits from a large amount of compression ([more information is available at the official apache parquet site](https://parquet.apache.org/)). 

Although parquet is quickly being adopted, exploring these files often requires opening another program (e.g. topcat) or programming language (`R` or `python`) to even take a quick look. Writing three or four lines of code just to see what is in a file is just too many.

`dog` is meant to be a clean way of quickly inspecting .parquet files in the terminal, in the same way that might done using `cat`. `cat` concatenates text files and prints them to standard output; it's quite commonly used in terminal environments to check the contents of files such as .csv or .txt.

However, `cat` is not useful for some file formats which are not simple text files (e.g. .fits or .parquet). 

`dog` aims to be an alternative to `cat` for these kinds of files. 

# Installation	
There are multiple ways to install dog including using `cargo`, installing the binary directory, and building from source.

## Cargo
This is very very very easy.

If you have rust and `cargo` installed then you can simply install dog with 
```
cargo install dog-tsl
```
that's it. The install might take a while as some optimizations take place.

If you don't have rust for some reason and want to install it (and `cargo`) then you can *easily* do that with
```
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```
Or read the [rust install documentation](https://rust-lang.org/tools/install/). The rust environment is made to be easy to use. So embrace it.

There are some known issues with older macs whose Xcode is no longer supported because they have intel chips. In these cases cargo install won't work and you can download the appropriate binary below. 

## Downloading the binary
The first step is to determine what architecture your system is running. This can be done with 
`uname -m` in the terminal. 

### Mac-OS
For Mac-OS the binaries can be downloaded for newer macs running m-chips (arm64)
**Installing dog is very easy**
```
curl -L -o dog https://github.com/trystanscottlambert/dog/releases/download/v1.0.0/dog-aarch64-apple-darwin
chmod +x dog
sudo mv dog /usr/local/bin/
```

For older models then:
```
curl -L -o dog https://github.com/trystanscottlambert/dog/releases/download/v1.0.0/dog-x86_64-apple-darwin
chmod +x dog
sudo mv dog /usr/local/bin/
```

should work. 

### Linux
Ubuntu/debian flavors of linux should work with:
```
curl -L -o dog https://github.com/trystanscottlambert/dog/releases/download/v1.0.0/dog-x86_64-unknown-linux-gnu
chmod +x dog
sudo mv dog /usr/local/bin/
```

You may need to start a new terminal to get it working.

If you don't want to install the binary or are running a more exotic distribution of linux, then you can compile the program from source using 'cargo'. 

## Compile from source

If you are using Linux or don't want to download a binary file then `dog` can be built from source using cargo. 

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
Often, it is useful to get the full names of the columns in a file. This can be done with `-n --names`
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

### Selecting certain rows using sql-like selection
`dog` supports sql-like filtering to select rows which can be used in combination with many of the other commands by using the `-f` `--filter` command. For example, if we only want to look at the parquet file where declination is positive and all redshifts are less than 0.2 then we can do:
```
dog -f 'dec>0 and redshift < 0.2' example.parquet 
```
This can then be used in combination with other commands.

### Saving a sub-sample
The `-o` `--outfile` flag can be used to write a parquet file. This will only be done if `-c` or `-f`  have been used to select columns and rows. So if we wanted to only have the ra and dec of all galaxies that are below a redshift of 0.2 then 
```
dog -c ra,dec -f 'redshift < 0' -o subsample.parquet mainsample.parquet
```
will do it. 


### Summary
A summary of the entire contents is available with the `-s --summary` option. 

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
```bash
dog -p test_file.parquet
```
However, more often than not, columns and rows will be emitted except for a couple at the corners.

### Stats
`dog` can generate basic summary statistics for a subset or all columns. 

```bash
dog --stats test_file.parquet # stats for all columns
dog -c ra,dec --stats test_file.parquet #just the stats for these columns
```

```
ra:
---------------
min: 0.000039
mean: 187.804214
median: 191.855545
max: 359.999969
std: 121.959641
null counts: 0

dec:
---------------
min: -35.599991
mean: -18.074331
median: -28.299042
max: 3.949996
std: 15.738805
null counts: 0
```


### Schema
The schema in the metadata of the parquet file can also be printed, but in this case this is only the schema which might be incomplete. 
```
dog --schema test_file.parquet
```

### Keyword metadata
Parquet files can store arbitrary key-value metadata in their footer, and `dog` can read, write, list, and delete these entries.

`dog` has specific support for "Meta YAML" or "MAML" (see: https://github.com/asgr/MAML). This is a structured metadata for astronomical surveys like [WAVES](https://wavesurvey.org/) and [4HS](https://4mosthemispheresurvey.github.io/). If this metadata exists then it can be viewed with the `-k` `--keyword` flag by asking for the `maml` keyword.

```bash
dog -k maml test_file.parquet
```
This is a useful way to strip MAML metadata from a parquet file
```bash
dog -k maml test_file.parquet > test.maml
```
The same flag works for any keyword, not just `maml`. If the keyword isn't in the file then nothing is printed and a warning is given.

#### Inserting metadata
Users can insert the contents of any text file into a parquet file under a keyword of their choosing. Though consider if this is what you really want to do; usually this step would be done officially at some point and inserting metadata runs the risk of overwriting valid metadata and even corruption.

The `--insert-metadata` flag takes the file to insert and the keyword to store it under, followed by the parquet file(s):

```
dog --insert-metadata galaxies_meta.maml maml galaxies.parquet
```

Because the keyword is arbitrary, a file can hold as many entries as are useful. A markdown description of a table might go in alongside the maml:

```
dog --insert-metadata galaxies_description.md table_description galaxies.parquet
```

If the file already has an entry at that keyword (like the first example does now) then you can forcefully overwrite it using the `-F` tag:

```
dog -F --insert-metadata different_galaxies_meta.maml maml galaxies.parquet
```

Else you will get an error.

Multiple files can be given at once, which means globs work as expected. This will insert the same description into every parquet in the directory:

```
dog --insert-metadata survey_description.md rip_description *.parquet
```

#### Listing keywords
To see which keywords a file actually has, use `--list-keywords`:

```bash
dog --list-keywords test_file.parquet
```
This prints every key in the file's key-value metadata, one per line. Note that this includes entries written by the tools that created the file (such as `ARROW:schema`) and not just those inserted by `dog`.

#### Deleting a keyword
A keyword and its contents can be removed with `--delete-keyword`:

```bash
dog --delete-keyword maml test_file.parquet
```
If the keyword isn't in the file then nothing is deleted and a warning is printed, so running this across a glob won't stop at the first file that happens to lack the keyword.


### Reading non-parquet and converting files
`dog` is built with parquet in mind, however, it can also read fits tables and csv tables in the exact same way as above. 
```bash
dog test.fits
dog test.csv
dog -p another_test.fits
dog -t another_test.csv
```

In addition to reading these other files which are quite common, these files can also be converted easily to parquet using the --convert flag.

```bash
dog --convert test.fits
```
will convert the .fits table to parquet and create a file called `test_converted.parquet`.
