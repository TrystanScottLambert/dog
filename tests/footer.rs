use assert_cmd::Command;
use std::{fs, path::PathBuf};
use tempfile::{tempdir, TempDir};

/// Helper function which moves the pristine files to a temp test bed
fn copy_test_files() -> (TempDir, PathBuf, PathBuf) {
    let dir = tempdir().expect("create temp dir");
    let parquet_path = dir.path().join("test.parquet");
    let maml_path = dir.path().join("test.maml");

    fs::copy("tests/fixtures/test.maml", &maml_path).expect("failed to copy maml file to temp dir");
    fs::copy("tests/fixtures/test.parquet", &parquet_path)
        .expect("failed to copy parquet file to temp dir");
    (dir, parquet_path, maml_path)
}

#[test]
fn markdown() {
    let dir = tempdir().expect("failed to create temp dir");
    let markdown_path = dir.path().join("test.md");
    let parquet_path = dir.path().join("test.parquet");
    fs::copy("tests/fixtures/test.md", &markdown_path).expect("Failed to copy test.md file.");
    fs::copy("tests/fixtures/test.parquet", &parquet_path)
        .expect("Failed to copy test.parquet file");

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata")
        .arg(&markdown_path)
        .arg("markdown")
        .arg(&parquet_path)
        .assert()
        .success();

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg(&parquet_path).assert().success();

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("-k")
        .arg("markdown")
        .arg(&parquet_path)
        .assert()
        .success();
}

#[test]
fn no_maml_exist() {
    let (_dir, parquet, maml) = copy_test_files();
    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata")
        .arg(&maml)
        .arg("maml")
        .arg(&parquet)
        .assert()
        .success();

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg(&parquet).assert().success(); // check file isn't corrupted
}

#[test]
fn maml_exists_without_force_fails() {
    let (_dir, parquet, maml) = copy_test_files();
    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata")
        .arg(&maml)
        .arg("maml")
        .arg(&parquet)
        .assert()
        .success();

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata")
        .arg(&maml)
        .arg("maml")
        .arg(&parquet)
        .assert()
        .failure();
}

#[test]
fn maml_exists_with_force() {
    let (_dir, parquet, maml) = copy_test_files();
    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata")
        .arg(&maml)
        .arg("maml")
        .arg(&parquet)
        .assert()
        .success();

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata")
        .arg(&maml)
        .arg("maml")
        .arg("-F")
        .arg(&parquet)
        .assert()
        .success();

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg(&parquet).assert().success(); // check file isn't corrupted
}
