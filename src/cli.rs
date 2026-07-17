use clap::{Arg, ArgAction, ArgGroup, Command};

pub fn build_cli() -> Command {
    Command::new("dog")
        .about("Parquet File Reader CLI")
        .arg(
            Arg::new("file")
                .required(true)
                .index(1) // this will always be the first positional argument
                .num_args(1..)
                .value_name("FILE")
                .help("Input <FILE>."),
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
            Arg::new("force")
                .short('F')
                .long("force")
                .help("Overwrite existing keyword metadata if it is already present.")
                .action(ArgAction::SetTrue)
                .requires("insert-metadata"),
        )
        .arg(
            Arg::new("tail")
                .short('t')
                .long("tail")
                .help("Prints the bottom <N> rows of data.")
                .num_args(1)
                .value_name("N"),
        )
        .arg(
            Arg::new("head")
                .short('H')
                .long("head")
                .help("Prints the top <N> rows of data and the column names.")
                .num_args(1)
                .value_name("N"),
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
                .help("Prints only the selected <COLUMN> by name. Multiple columns can be comma separated.")
                .num_args(1)
                .value_name("COLUMN")
                .conflicts_with_all(["convert", "insert-metadata", "schema", "keyword"])
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
            Arg::new("insert-metadata")
                .long("insert-metadata")
                .help("Inserts contents of a file a <METADATA-FILE> into the parquet file at a given <KEYWORD> header position.")
                .num_args(2)
                .value_names(["METADATA-FILE", "KEYWORD"]),
        )
        .arg(
            Arg::new("keyword")
                .short('k')
                .long("keyword")
                .help("Print the <KEYWORD> metadata if it exists.")
                .num_args(1)
                .value_name("KEYWORD")
        )
        .arg(
            Arg::new("list-keyword-metadata")
            .long("list-keyword")
            .help("Lists the keyword metadata of the given parquet file")
            .action(ArgAction::SetTrue)
        )
        .arg(
            Arg::new("delete-keyword-metadata")
            .long("delete-keyword")
            .help("Deletes the fiven <KEYWORD> metadata from the header if it exists.")
            .num_args(1)
            .value_name("KEYWORD")
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
                .help("Filter rows based on some <SQL-STATEMENT>. E.g. ra<10.")
                .num_args(1)
                .value_name("SQL-STATEMENT")
                .conflicts_with_all(["convert", "insert-metadata", "schema", "keyword"]),
        )
        .arg(
            Arg::new("outfile")
                .long("outfile")
                .short('o')
                .help("Save the current selection to the <OUTFILE>.")
                .num_args(1)
                .value_name("OUTFILE"),
        )
        .group(
            ArgGroup::new("mode")
                .args([
                    "names",
                    "data",
                    "tail",
                    "head",
                    "convert",
                    "insert-metadata",
                    "delete-keyword-metadata",
                    "list-keyword-metadata",
                    "summary",
                    "peak",
                    "stats",
                    "keyword",
                    "schema",
                    "outfile",
                ])
                .multiple(false),
        )
}
