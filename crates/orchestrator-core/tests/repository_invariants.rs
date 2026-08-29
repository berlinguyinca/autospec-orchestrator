use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("workspace root")
        .to_path_buf()
}

fn collect_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read audited directory") {
        let path = entry.expect("read audited entry").path();
        if path.is_dir() {
            collect_files(&path, files);
        } else {
            files.push(path);
        }
    }
}

fn executable_sources() -> Vec<PathBuf> {
    let root = repository_root();
    let mut files = Vec::new();
    for entry in fs::read_dir(root.join("crates")).expect("read crates") {
        let crate_root = entry.expect("read crate").path();
        let source = crate_root.join("src");
        if source.is_dir() {
            collect_files(&source, &mut files);
        }
        for name in ["build.rs", "Dockerfile"] {
            let path = crate_root.join(name);
            if path.is_file() {
                files.push(path);
            }
        }
    }
    for directory in ["deploy", "scripts", ".github/workflows"] {
        let path = root.join(directory);
        if path.is_dir() {
            collect_files(&path, &mut files);
        }
    }
    for entry in fs::read_dir(&root).expect("read workspace root") {
        let path = entry.expect("read workspace entry").path();
        if !path.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        let executable =
            fs::metadata(&path).is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0);
        if name.starts_with("Dockerfile")
            || matches!(name, "Makefile" | "Justfile")
            || name.ends_with(".sh")
            || executable
        {
            files.push(path);
        }
    }
    files.sort();
    files.dedup();
    files
}

fn logical_lines(path: &Path) -> Vec<(usize, String)> {
    let source = fs::read_to_string(path).expect("read auditable UTF-8 source");
    let mut logical = Vec::new();
    let mut pending = String::new();
    let mut start = 1;
    for (index, line) in source.lines().enumerate() {
        let trimmed = line.trim_start();
        if pending.is_empty() && (trimmed.starts_with("//") || trimmed.starts_with('#')) {
            continue;
        }
        if pending.is_empty() {
            start = index + 1;
        }
        let continued = line.trim_end().ends_with('\\');
        pending.push_str(
            line.trim_end_matches(|character: char| character == '\\' || character.is_whitespace()),
        );
        pending.push(' ');
        if !continued {
            logical.push((
                start,
                pending.split_whitespace().collect::<Vec<_>>().join(" "),
            ));
            pending.clear();
        }
    }
    if !pending.is_empty() {
        logical.push((
            start,
            pending.split_whitespace().collect::<Vec<_>>().join(" "),
        ));
    }
    logical
}

fn scan_files(files: Vec<PathBuf>) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files {
        let lines = logical_lines(&path);
        for (line_number, line) in &lines {
            let lower = line.to_ascii_lowercase();
            let compact = lower
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>();
            for operation in ["selectgpu(", "loadmodel(", "gpuqueue(", "modelplacement("] {
                if compact.contains(operation) {
                    violations.push(format!(
                        "{}:{line_number} invokes {operation}",
                        path.display()
                    ));
                }
            }
            let tokens = lower
                .split(|character: char| {
                    character.is_whitespace() || matches!(character, '"' | '\'' | ',' | ';')
                })
                .filter(|token| !token.is_empty())
                .collect::<Vec<_>>();
            if tokens
                .iter()
                .position(|token| *token == "docker")
                .is_some_and(|docker| {
                    tokens[docker + 1..]
                        .iter()
                        .take(4)
                        .any(|token| *token == "prune")
                })
            {
                violations.push(format!(
                    "{}:{line_number} invokes global Docker pruning",
                    path.display()
                ));
            }
            let git_branch = lower.find("git branch");
            if git_branch.is_some()
                && lower.contains("grep")
                && lower.contains("xargs")
                && lower.contains('|')
            {
                violations.push(format!(
                    "{}:{line_number} invokes global Git branch deletion",
                    path.display()
                ));
            }
        }
        let joined = lines
            .iter()
            .map(|(_, line)| line.as_str())
            .collect::<Vec<_>>()
            .join(" ");
        let compact = joined
            .to_ascii_lowercase()
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        if let Some(command) = compact.find("command::new(\"docker\")") {
            let tail = &compact[command..compact.len().min(command + 512)];
            if tail.contains("\"prune\"") {
                let line_number = lines
                    .iter()
                    .find(|(_, line)| line.to_ascii_lowercase().contains("command::new"))
                    .map_or(1, |(line, _)| *line);
                violations.push(format!(
                    "{}:{line_number} constructs global Docker pruning",
                    path.display()
                ));
            }
        }
    }
    violations
}

#[test]
fn executable_sources_never_gain_global_cleanup_or_inference_placement() {
    let violations = scan_files(executable_sources());

    assert!(
        violations.is_empty(),
        "forbidden execution-plane operations:\n{}",
        violations.join("\n")
    );
}

#[test]
fn forbidden_scanner_catches_shell_continuations_and_indirect_variants() {
    let fixture = repository_root()
        .join("crates/orchestrator-core/tests/fixtures/forbidden-scanner/forbidden");
    let mut files = Vec::new();
    collect_files(&fixture, &mut files);
    let violations = scan_files(files);
    assert_eq!(violations.len(), 5, "{violations:#?}");
    assert!(violations.iter().all(|violation| violation.contains(':')));
}

#[test]
fn forbidden_scanner_ignores_comments_and_bounded_cleanup() {
    let fixture =
        repository_root().join("crates/orchestrator-core/tests/fixtures/forbidden-scanner/allowed");
    let mut files = Vec::new();
    collect_files(&fixture, &mut files);
    assert!(scan_files(files).is_empty());
}
