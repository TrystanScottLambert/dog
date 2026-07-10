use anyhow::{anyhow, Result};
use polars::prelude::*;
use polars::sql::sql_expr;

pub fn parse_selection_string(input: &str) -> Result<Expr> {
    match sql_expr(input) {
        Ok(expr) => Ok(expr),
        _ => Err(anyhow!("Error parsing filter selection string.")),
    }
}
