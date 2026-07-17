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

/// Runs `dog <args...> <p1> <p2> ...` over every parquet in one invocation.
fn run_over_all(args: &[&str], parquets: &[PathBuf]) -> assert_cmd::assert::Assert {
    let mut cmd = Command::cargo_bin("dog").unwrap();
    cmd.env("NO_COLOR", "1").args(args);
    for p in parquets {
        cmd.arg(p);
    }
    cmd.assert()
}

/// Inserts `keyword` into a single parquet.
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

/// Inserts `keyword` into every parquet in one invocation.
fn insert_over_all(meta: &PathBuf, keyword: &str, parquets: &[PathBuf]) {
    run_over_all(
        &["--insert-metadata", meta.to_str().unwrap(), keyword],
        parquets,
    )
    .success();
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

#[test]
fn glob_list_keywords_covers_every_file() {
    let (_dir, parquets, maml) = copy_globbed_files(3);
    insert_over_all(&maml, "maml", &parquets);

    // one invocation across all files should not blow up
    run_over_all(&["--list-keywords"], &parquets).success();

    // and each file individually should show the key
    for p in &parquets {
        let listed = stdout_of(&["--list-keywords", p.to_str().unwrap()]);
        assert!(
            lists_keyword(&listed, "maml"),
            "'maml' missing from {}:\n{listed}",
            p.display()
        );
    }
}

#[test]
fn glob_delete_keyword_removes_from_every_file() {
    let (_dir, parquets, maml) = copy_globbed_files(3);
    insert_over_all(&maml, "maml", &parquets);

    run_over_all(&["--delete-keyword", "maml"], &parquets).success();

    for p in &parquets {
        let listed = stdout_of(&["--list-keywords", p.to_str().unwrap()]);
        assert!(
            !lists_keyword(&listed, "maml"),
            "'maml' survived delete in {}:\n{listed}",
            p.display()
        );
        assert_not_corrupted(p);
    }
}

#[test]
fn glob_delete_missing_keyword_does_not_stop_the_run() {
    let (_dir, parquets, maml) = copy_globbed_files(3);

    // only the middle file gets the keyword
    insert(&maml, "maml", &parquets[1]);

    // deleting across the whole glob should warn on 1 and 3 but still do 2
    run_over_all(&["--delete-keyword", "maml"], &parquets).success();

    for p in &parquets {
        let listed = stdout_of(&["--list-keywords", p.to_str().unwrap()]);
        assert!(
            !lists_keyword(&listed, "maml"),
            "'maml' survived delete in {}:\n{listed}",
            p.display()
        );
        assert_not_corrupted(p);
    }
}

#[test]
fn glob_get_keyword_missing_from_some_files_does_not_stop_the_run() {
    let (_dir, parquets, maml) = copy_globbed_files(3);

    // only the last file gets the keyword
    insert(&maml, "maml", &parquets[2]);

    // -k across the glob should skip the misses and still print the hit
    let got = stdout_of(&[
        "-k",
        "maml",
        parquets[0].to_str().unwrap(),
        parquets[1].to_str().unwrap(),
        parquets[2].to_str().unwrap(),
    ]);
    let expected = fs::read_to_string(&maml).unwrap();
    assert_eq!(
        got.trim_end(),
        expected.trim_end(),
        "-k over a partially-tagged glob did not return the one hit"
    );

    for p in &parquets {
        assert_not_corrupted(p);
    }
}

#[test]
fn glob_delete_one_keyword_leaves_others_intact() {
    let (dir, parquets, maml) = copy_globbed_files(3);
    let markdown_path = dir.path().join("test.md");
    fs::copy("tests/fixtures/test.md", &markdown_path).expect("Failed to copy test.md file.");

    insert_over_all(&maml, "maml", &parquets);
    insert_over_all(&markdown_path, "markdown", &parquets);

    run_over_all(&["--delete-keyword", "maml"], &parquets).success();

    for p in &parquets {
        let listed = stdout_of(&["--list-keywords", p.to_str().unwrap()]);
        assert!(
            !lists_keyword(&listed, "maml"),
            "'maml' survived in {}:\n{listed}",
            p.display()
        );
        assert!(
            lists_keyword(&listed, "markdown"),
            "deleting 'maml' also removed 'markdown' from {}:\n{listed}",
            p.display()
        );
        assert_keyword_matches(p, "markdown", &markdown_path);
        assert_not_corrupted(p);
    }
}

#[test]
fn glob_round_trip_insert_list_delete_list() {
    let (_dir, parquets, maml) = copy_globbed_files(3);

    insert_over_all(&maml, "maml", &parquets);
    for p in &parquets {
        let listed = stdout_of(&["--list-keywords", p.to_str().unwrap()]);
        assert!(lists_keyword(&listed, "maml"), "{}:\n{listed}", p.display());
    }

    run_over_all(&["--delete-keyword", "maml"], &parquets).success();
    for p in &parquets {
        let listed = stdout_of(&["--list-keywords", p.to_str().unwrap()]);
        assert!(
            !lists_keyword(&listed, "maml"),
            "{}:\n{listed}",
            p.display()
        );
    }

    // and the files still read fine after the round trip
    for p in &parquets {
        assert_not_corrupted(p);
        Command::cargo_bin("dog").unwrap().arg(p).assert().success();
    }
}
