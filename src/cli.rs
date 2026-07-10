// cli manager

use clap::{Arg, ArgAction, ArgGroup, Command};

pub fn build_cli() -> Command {
    Command::new("dog")
        .about("Parquet File Reader CLI")
        .arg(
            Arg::new("file")
                .required(true)
                .index(1) // this will always be the first positional argument
                .help("Input parquet file."),
        )
        .arg(
            Arg::new("names")
                .short('n')
                .long("names")
                .help("Prints only column names.")
                .action(ArgAction::SetTrue)
                .conflicts_with("data"),
        )
        .arg(
            Arg::new("data")
                .short('d')
                .long("data")
                .help("Prints only the data.")
                .action(ArgAction::SetTrue)
                .conflicts_with("names"),
        )
        .arg(
            Arg::new("insert-maml")
                .long("insert-maml")
                .help("Inserts MAML metadata from the given .maml file into the parquet file.")
                .num_args(1)
                .value_name("maml_file"),
        )
        .arg(
            Arg::new("force")
                .short('F')
                .long("force")
                .help("Overwrite existing MAML metadata if it is already present.")
                .action(ArgAction::SetTrue)
                .requires("insert-maml"),
        )
        .arg(
            Arg::new("tail")
                .short('t')
                .long("tail")
                .help("Prints the bottom ten rows of data.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("head")
                .short('H')
                .long("head")
                .help("Prints the top ten rows of data and the column names.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("schema")
                .long("schema")
                .help("Prints metadata schema.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("columns")
                .short('c')
                .long("columns")
                .help("Prints only the selected columns by name.")
                .num_args(1)
                .conflicts_with_all(["convert", "insert-maml", "schema", "maml"])
                .value_delimiter(','),
        )
        .arg(
            Arg::new("summary")
                .short('s')
                .long("summary")
                .help("Prints a summary of the Parquet file.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("peak")
                .short('p')
                .long("peak")
                .help("Peaks at the data. Prints a small table in polars format.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("maml")
                .short('w')
                .long("maml")
                .help("Print the MAML metadata if it exists.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("stats")
                .long("stats")
                .help("Summary statistics depending on column datatype.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("convert")
                .long("convert")
                .help("Attempts to convert csv and fits files into a parquet if it can.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("filter")
                .long("filter")
                .short('f')
                .help("Filter rows based on some selection. E.g. ra<10")
                .num_args(1)
                .value_name("sql-like row selection")
                .conflicts_with_all(["convert", "insert-maml", "schema", "maml"]),
        )
        .group(
            ArgGroup::new("mode")
                .args([
                    "names",
                    "data",
                    "tail",
                    "head",
                    "convert",
                    "insert-maml",
                    "summary",
                    "peak",
                    "stats",
                    "maml",
                    "schema",
                ])
                .multiple(false),
        )
}
