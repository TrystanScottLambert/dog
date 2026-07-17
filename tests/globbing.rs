use assert_cmd::Command;
use std::{fs, path::PathBuf};
use tempfile::{tempdir, TempDir};

fn copy_globbed_files(n: usize) -> (TempDir, Vec<PathBuf>, PathBuf) {
    let dir = tempdir().expect("create temp dir");
    let maml_path = dir.path().join("test.maml");
    fs::copy("tests/fixtures/test.maml", &maml_path).expect("failed to copy maml file to temp dir");

    let parquet_paths = (1..=n)
        .map(|i| {
            let p = dir.path().join(format!("test_{i}.parquet"));
            fs::copy("tests/fixtures/test.parquet", &p)
                .expect("failed to copy parquet to temp dir");
            p
        })
        .collect();

    (dir, parquet_paths, maml_path)
}

/// Asserts `dog -k <keyword> <file>` returns exactly the contents of `expected_file`.
fn assert_keyword_matches(parquet: &PathBuf, keyword: &str, expected_file: &PathBuf) {
    let expected = fs::read_to_string(expected_file).expect("failed to read expected file");

    let output = Command::cargo_bin("dog")
        .unwrap()
        .arg("-k")
        .arg(keyword)
        .arg(parquet)
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();

    let actual = String::from_utf8(output).expect("stdout was not utf8");
    assert_eq!(
        actual.trim_end(),
        expected.trim_end(),
        "keyword '{keyword}' in {} did not match {}",
        parquet.display(),
        expected_file.display()
    );
}

#[test]
fn glob_insert_maml_all_files() {
    let (_dir, parquets, maml) = copy_globbed_files(3);

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata").arg(&maml).arg("maml");
    for p in &parquets {
        cmd.arg(p);
    }
    cmd.assert().success();

    for p in &parquets {
        assert_keyword_matches(p, "maml", &maml);
        // check the file isn't corrupted
        Command::cargo_bin("dog").unwrap().arg(p).assert().success();
    }
}

#[test]
fn glob_insert_markdown_all_files() {
    let (dir, parquets, _maml) = copy_globbed_files(3);
    let markdown_path = dir.path().join("test.md");
    fs::copy("tests/fixtures/test.md", &markdown_path).expect("Failed to copy test.md file.");

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata")
        .arg(&markdown_path)
        .arg("markdown");
    for p in &parquets {
        cmd.arg(p);
    }
    cmd.assert().success();

    for p in &parquets {
        assert_keyword_matches(p, "markdown", &markdown_path);
        Command::cargo_bin("dog").unwrap().arg(p).assert().success();
    }
}

#[test]
fn glob_insert_without_force_fails_on_second_run() {
    let (_dir, parquets, maml) = copy_globbed_files(3);

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata").arg(&maml).arg("maml");
    for p in &parquets {
        cmd.arg(p);
    }
    cmd.assert().success();

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata").arg(&maml).arg("maml");
    for p in &parquets {
        cmd.arg(p);
    }
    cmd.assert().failure();
}

#[test]
fn glob_insert_with_force_overwrites_all_files() {
    let (_dir, parquets, maml) = copy_globbed_files(3);

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata").arg(&maml).arg("maml");
    for p in &parquets {
        cmd.arg(p);
    }
    cmd.assert().success();

    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.arg("--insert-metadata")
        .arg(&maml)
        .arg("maml")
        .arg("-F");
    for p in &parquets {
        cmd.arg(p);
    }
    cmd.assert().success();

    for p in &parquets {
        assert_keyword_matches(p, "maml", &maml);
        Command::cargo_bin("dog").unwrap().arg(p).assert().success();
    }
}

#[test]
fn glob_multiple_keywords_coexist() {
    let (dir, parquets, maml) = copy_globbed_files(3);
    let markdown_path = dir.path().join("test.md");
    fs::copy("tests/fixtures/test.md", &markdown_path).expect("Failed to copy test.md file.");

    for (meta, keyword) in [(&maml, "maml"), (&markdown_path, "markdown")] {
        let mut cmd = Command::cargo_bin("dog").unwrap();
        cmd.arg("--insert-metadata").arg(meta).arg(keyword);
        for p in &parquets {
            cmd.arg(p);
        }
        cmd.assert().success();
    }

    for p in &parquets {
        assert_keyword_matches(p, "maml", &maml);
        assert_keyword_matches(p, "markdown", &markdown_path);
    }
}
