use std::{
    fs,
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
        let source = entry.expect("read crate").path().join("src");
        if source.is_dir() {
            collect_files(&source, &mut files);
        }
    }
    collect_files(&root.join("deploy"), &mut files);
    files
}

fn uncommented_lines(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .expect("read auditable UTF-8 source")
        .lines()
        .filter_map(|line| {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") || trimmed.starts_with('#') {
                None
            } else {
                Some(line.to_owned())
            }
        })
        .collect()
}

#[test]
fn executable_sources_never_gain_global_cleanup_or_inference_placement() {
    let forbidden_literals = ["selectGpu(", "loadModel(", "gpuQueue(", "modelPlacement("];
    let mut violations = Vec::new();

    for path in executable_sources() {
        let lines = uncommented_lines(&path);
        let source = lines.join("\n");
        for literal in forbidden_literals {
            if source.contains(literal) {
                violations.push(format!("{} contains {literal}", path.display()));
            }
        }
        for (index, line) in lines.iter().enumerate() {
            let normalized = line.split_whitespace().collect::<Vec<_>>().join(" ");
            if normalized.contains("docker ") && normalized.contains(" prune") {
                violations.push(format!(
                    "{}:{} invokes global Docker pruning",
                    path.display(),
                    index + 1
                ));
            }
            if normalized.contains("git branch")
                && normalized.contains("grep")
                && normalized.contains("xargs")
            {
                violations.push(format!(
                    "{}:{} invokes global Git branch deletion",
                    path.display(),
                    index + 1
                ));
            }
        }
        if source.contains("\"prune\"") && source.contains("Command::new(\"docker\")") {
            violations.push(format!(
                "{} constructs a Docker prune command",
                path.display()
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "forbidden execution-plane operations:\n{}",
        violations.join("\n")
    );
}
