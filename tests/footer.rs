use assert_cmd::Command;
use predicates::prelude::*;
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

/// Copies the markdown fixture in alongside the standard test bed.
fn copy_markdown(dir: &TempDir) -> PathBuf {
    let markdown = dir.path().join("test.md");
    fs::copy("tests/fixtures/test.md", &markdown).expect("Failed to copy test.md file.");
    markdown
}

/// Returns stdout of a successful `dog` invocation as a String.
fn stdout_of(args: &[&str]) -> String {
    let output = Command::cargo_bin("dog")
        .unwrap()
        .env("NO_COLOR", "1")
        .args(args)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    String::from_utf8(output).expect("stdout was not utf8")
}

/// True if `keyword` appears as its own line in `--list-keywords` output.
fn lists_keyword(listed: &str, keyword: &str) -> bool {
    listed.lines().any(|l| l.trim() == keyword)
}

/// `dog -p <file>` should always work on a non-corrupt parquet.
fn assert_not_corrupted(parquet: &PathBuf) {
    Command::cargo_bin("dog")
        .unwrap()
        .arg("-p")
        .arg(parquet)
        .assert()
        .success();
}

fn insert(meta: &PathBuf, keyword: &str, parquet: &PathBuf) {
    Command::cargo_bin("dog")
        .unwrap()
        .arg("--insert-metadata")
        .arg(meta)
        .arg(keyword)
        .arg(parquet)
        .assert()
        .success();
}

fn delete(keyword: &str, parquet: &PathBuf) {
    Command::cargo_bin("dog")
        .unwrap()
        .arg("--delete-keyword")
        .arg(keyword)
        .arg(parquet)
        .assert()
        .success();
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

#[test]
fn list_keywords_shows_inserted_keyword() {
    let (_dir, parquet, maml) = copy_test_files();
    insert(&maml, "maml", &parquet);

    let listed = stdout_of(&["--list-keywords", parquet.to_str().unwrap()]);
    assert!(
        lists_keyword(&listed, "maml"),
        "expected 'maml' in --list-keywords output, got:\n{listed}"
    );
}

#[test]
fn list_keywords_shows_all_inserted_keywords() {
    let (dir, parquet, maml) = copy_test_files();
    let markdown = copy_markdown(&dir);

    insert(&maml, "maml", &parquet);
    insert(&markdown, "markdown", &parquet);

    let listed = stdout_of(&["--list-keywords", parquet.to_str().unwrap()]);
    assert!(lists_keyword(&listed, "maml"), "missing 'maml':\n{listed}");
    assert!(
        lists_keyword(&listed, "markdown"),
        "missing 'markdown':\n{listed}"
    );
}

#[test]
fn round_trip_insert_list_delete_list() {
    let (dir, parquet, maml) = copy_test_files();
    let markdown = copy_markdown(&dir);

    insert(&maml, "maml", &parquet);
    insert(&markdown, "markdown", &parquet);

    let before = stdout_of(&["--list-keywords", parquet.to_str().unwrap()]);
    assert!(lists_keyword(&before, "maml"), "missing 'maml':\n{before}");
    assert!(
        lists_keyword(&before, "markdown"),
        "missing 'markdown':\n{before}"
    );

    delete("maml", &parquet);

    let after = stdout_of(&["--list-keywords", parquet.to_str().unwrap()]);
    assert!(
        !lists_keyword(&after, "maml"),
        "'maml' still listed after delete:\n{after}"
    );
    assert!(
        lists_keyword(&after, "markdown"),
        "delete of 'maml' also removed 'markdown':\n{after}"
    );

    // the deleted keyword should no longer be retrievable
    let got = stdout_of(&["-k", "maml", parquet.to_str().unwrap()]);
    assert!(
        got.trim().is_empty(),
        "-k maml returned content after delete:\n{got}"
    );

    // the surviving keyword should be untouched
    let expected = fs::read_to_string(&markdown).unwrap();
    let got = stdout_of(&["-k", "markdown", parquet.to_str().unwrap()]);
    assert_eq!(got.trim_end(), expected.trim_end());

    assert_not_corrupted(&parquet);
}

#[test]
fn delete_then_reinsert_without_force_succeeds() {
    let (_dir, parquet, maml) = copy_test_files();
    insert(&maml, "maml", &parquet);
    delete("maml", &parquet);

    // the key is gone, so a plain insert should not trip the -F guard
    insert(&maml, "maml", &parquet);

    let expected = fs::read_to_string(&maml).unwrap();
    let got = stdout_of(&["-k", "maml", parquet.to_str().unwrap()]);
    assert_eq!(got.trim_end(), expected.trim_end());
    assert_not_corrupted(&parquet);
}

#[test]
fn delete_missing_keyword_warns_but_succeeds() {
    let (_dir, parquet, _maml) = copy_test_files();

    Command::cargo_bin("dog")
        .unwrap()
        .env("NO_COLOR", "1")
        .arg("--delete-keyword")
        .arg("not_a_real_keyword")
        .arg(&parquet)
        .assert()
        .success()
        .stderr(predicate::str::contains("not_a_real_keyword"));

    assert_not_corrupted(&parquet);
}

#[test]
fn delete_all_inserted_keywords_leaves_readable_file() {
    let (dir, parquet, maml) = copy_test_files();
    let markdown = copy_markdown(&dir);

    insert(&maml, "maml", &parquet);
    insert(&markdown, "markdown", &parquet);

    delete("maml", &parquet);
    delete("markdown", &parquet);

    let listed = stdout_of(&["--list-keywords", parquet.to_str().unwrap()]);
    assert!(
        !lists_keyword(&listed, "maml"),
        "'maml' survived:\n{listed}"
    );
    assert!(
        !lists_keyword(&listed, "markdown"),
        "'markdown' survived:\n{listed}"
    );

    assert_not_corrupted(&parquet);
    Command::cargo_bin("dog")
        .unwrap()
        .arg(&parquet)
        .assert()
        .success();
}
