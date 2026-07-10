use assert_cmd::Command;
use polars::{df, prelude::ParquetWriter};
use predicates::prelude::*;
use tempfile::{Builder, NamedTempFile};

fn create_test_parquet() -> NamedTempFile {
    let mut df = df! [
        "ra" => [100., 200., 300.],
        "dec" => [-20., 0., 20.],
        "redshift" => [None, Some(1.), Some(0.2)],
    ]
    .unwrap();
    let mut file = Builder::new().suffix(".parquet").tempfile().unwrap();
    ParquetWriter::new(file.as_file_mut())
        .finish(&mut df)
        .unwrap();
    file
}

fn run(file: &NamedTempFile, filter: &str) -> assert_cmd::assert::Assert {
    Command::cargo_bin("dog")
        .unwrap()
        .arg("--filter")
        .arg(filter)
        .arg(file.path())
        .assert()
}

#[test]
fn test_simple_comparison() {
    let f = create_test_parquet();
    run(&f, "ra > 100")
        .success()
        .stdout(predicate::str::contains("200"))
        .stdout(predicate::str::contains("300"))
        .stdout(predicate::str::contains("100").not());
}

#[test]
fn test_is_null() {
    let f = create_test_parquet();
    // only the ra=100 row has a null redshift
    run(&f, "redshift IS NULL")
        .success()
        .stdout(predicate::str::contains("100"))
        .stdout(predicate::str::contains("200").not());
}

#[test]
fn test_is_not_null() {
    let f = create_test_parquet();
    run(&f, "redshift IS NOT NULL")
        .success()
        .stdout(predicate::str::contains("200"))
        .stdout(predicate::str::contains("300"))
        .stdout(predicate::str::contains("100").not());
}

#[test]
fn test_null_excluded_by_comparison() {
    let f = create_test_parquet();
    // three-valued logic: the null row is neither < 0.5 nor >= 0.5
    run(&f, "redshift < 0.5")
        .success()
        .stdout(predicate::str::contains("300"))
        .stdout(predicate::str::contains("100").not());
}

#[test]
fn test_and() {
    let f = create_test_parquet();
    run(&f, "ra > 100 AND dec < 20")
        .success()
        .stdout(predicate::str::contains("200"))
        .stdout(predicate::str::contains("300").not());
}

#[test]
fn test_or() {
    let f = create_test_parquet();
    run(&f, "ra = 100 OR redshift < 0.5")
        .success()
        .stdout(predicate::str::contains("100"))
        .stdout(predicate::str::contains("300"))
        .stdout(predicate::str::contains("200").not());
}

#[test]
fn test_precedence_with_parens() {
    let f = create_test_parquet();
    run(&f, "(ra = 100 OR ra = 200) AND dec < 0")
        .success()
        .stdout(predicate::str::contains("100"))
        .stdout(predicate::str::contains("200").not());
}

#[test]
fn test_garbage_fails() {
    let f = create_test_parquet();
    run(&f, "this is not sql (((").failure();
}

#[test]
fn test_unknown_column_fails() {
    let f = create_test_parquet();
    run(&f, "nonexistent > 5").failure();
}

#[test]
fn test_dangling_operator_fails() {
    let f = create_test_parquet();
    run(&f, "ra >").failure();
}
