// cli manager

use clap::{Arg, ArgAction, Command};

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
            Arg::new("tail")
                .short('t')
                .long("tail")
                .help("Prints the bottom ten rows of data.")
                .action(ArgAction::SetTrue)
                .conflicts_with("head"),
        )
        .arg(
            Arg::new("head")
                .short('H')
                .long("head")
                .help("Prints the top ten rows of data and the column names.")
                .action(ArgAction::SetTrue)
                .conflicts_with("tail"),
        )
        .arg(
            Arg::new("META")
                .short('M')
                .long("META")
                .help("Forcefully prints metadata schema without any formatting.")
                .action(ArgAction::SetTrue),
        )
        .arg(
            Arg::new("columns")
                .short('c')
                .long("columns")
                .help("Prints only the selected columns by name.")
                .num_args(1)
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
}
