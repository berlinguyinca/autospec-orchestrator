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

fn shell_like(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("");
    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("sh" | "yml" | "yaml")
    ) || name.starts_with("Dockerfile")
        || matches!(name, "Makefile" | "Justfile")
        || fs::metadata(path).is_ok_and(|metadata| metadata.permissions().mode() & 0o111 != 0)
}

fn rust_source(path: &Path) -> bool {
    path.extension().and_then(|extension| extension.to_str()) == Some("rs")
        || path.file_name().and_then(|name| name.to_str()) == Some("build.rs")
}

fn logical_lines(path: &Path) -> Vec<(usize, String)> {
    let source = fs::read_to_string(path).expect("read auditable UTF-8 source");
    let mut logical = Vec::new();
    let mut pending = String::new();
    let mut start = 1;
    let hash_comments = shell_like(path);
    for (index, line) in source.lines().enumerate() {
        let mut code = String::new();
        let mut characters = line.chars().peekable();
        let mut quoted = None::<char>;
        while let Some(character) = characters.next() {
            if let Some(quote) = quoted {
                code.push(character);
                if character == quote {
                    quoted = None;
                } else if character == '\\' {
                    if let Some(escaped) = characters.next() {
                        code.push(escaped);
                    }
                }
                continue;
            }
            if character == '"' || (hash_comments && character == '\'') {
                quoted = Some(character);
                code.push(character);
            } else if character == '#' && hash_comments {
                break;
            } else {
                code.push(character);
            }
        }
        let trimmed = code.trim_start();
        if pending.is_empty() && (trimmed.starts_with("//") || trimmed.starts_with('#')) {
            continue;
        }
        if trimmed.is_empty() {
            continue;
        }
        if pending.is_empty() {
            start = index + 1;
        }
        let continued = code.trim_end().ends_with('\\');
        pending.push_str(
            code.trim_end_matches(|character: char| character == '\\' || character.is_whitespace()),
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

fn shell_tokens(line: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut token = String::new();
    let mut characters = line.chars().peekable();
    let mut quote = None;
    while let Some(character) = characters.next() {
        if let Some(delimiter) = quote {
            if character == delimiter {
                quote = None;
            } else if character == '\\' && delimiter == '"' {
                if let Some(escaped) = characters.next() {
                    token.push(escaped);
                }
            } else {
                token.push(character);
            }
            continue;
        }
        if matches!(character, '"' | '\'') {
            quote = Some(character);
        } else if character.is_whitespace() || character == ',' {
            if !token.is_empty() {
                tokens.push(std::mem::take(&mut token));
            }
        } else if matches!(character, '&' | '|' | ';') {
            if !token.is_empty() {
                tokens.push(std::mem::take(&mut token));
            }
            let mut operator = character.to_string();
            if matches!(character, '&' | '|') && characters.peek() == Some(&character) {
                operator.push(characters.next().expect("peeked operator"));
            }
            tokens.push(operator);
        } else {
            token.push(character);
        }
    }
    if !token.is_empty() {
        tokens.push(token);
    }
    tokens
}

fn is_control_operator(token: &str) -> bool {
    matches!(token, "&&" | "||" | ";" | "|")
}

fn is_docker_command(token: &str) -> bool {
    let lower = token
        .trim_matches(|character: char| matches!(character, '(' | ')' | '[' | ']' | '{' | '}'))
        .to_ascii_lowercase();
    if lower.contains("://") {
        return false;
    }
    let basename = lower.rsplit('/').next().unwrap_or(&lower);
    basename == "docker"
        || basename.starts_with("docker-")
        || basename.starts_with("docker_")
        || (basename.starts_with('$') && basename.contains("docker"))
}

fn shell_invokes_docker_prune(tokens: &[String]) -> bool {
    tokens
        .split(|token| is_control_operator(token))
        .any(|command| {
            let lower = command
                .iter()
                .map(|token| token.to_ascii_lowercase())
                .collect::<Vec<_>>();
            lower.iter().enumerate().any(|(family, token)| {
                matches!(
                    token.as_str(),
                    "system" | "container" | "image" | "network" | "volume"
                ) && lower.get(family + 1).is_some_and(|token| token == "prune")
                    && command[..family]
                        .iter()
                        .any(|token| is_docker_command(token))
            })
        })
}

fn closes_raw_string(bytes: &[u8], quote: usize, hashes: usize) -> bool {
    bytes.get(quote) == Some(&b'"')
        && quote + 1 + hashes <= bytes.len()
        && bytes[quote + 1..quote + 1 + hashes]
            .iter()
            .all(|byte| *byte == b'#')
}

fn mask_rust_non_code(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut masked = bytes.to_vec();
    let mut index = 0;
    let mut block_depth = 0_u32;
    while index < bytes.len() {
        if block_depth > 0 {
            if bytes[index..].starts_with(b"/*") {
                masked[index] = b' ';
                masked[index + 1] = b' ';
                block_depth += 1;
                index += 2;
            } else if bytes[index..].starts_with(b"*/") {
                masked[index] = b' ';
                masked[index + 1] = b' ';
                block_depth -= 1;
                index += 2;
            } else {
                if bytes[index] != b'\n' {
                    masked[index] = b' ';
                }
                index += 1;
            }
            continue;
        }
        if bytes[index..].starts_with(b"//") {
            while index < bytes.len() && bytes[index] != b'\n' {
                masked[index] = b' ';
                index += 1;
            }
        } else if bytes[index..].starts_with(b"/*") {
            masked[index] = b' ';
            masked[index + 1] = b' ';
            block_depth = 1;
            index += 2;
        } else if bytes[index] == b'r' {
            let mut quote = index + 1;
            while quote < bytes.len() && bytes[quote] == b'#' {
                quote += 1;
            }
            if quote < bytes.len() && bytes[quote] == b'"' {
                let hashes = quote - index - 1;
                let mut end = quote + 1;
                while end < bytes.len() {
                    if closes_raw_string(bytes, end, hashes) {
                        end += hashes + 1;
                        break;
                    }
                    end += 1;
                }
                for position in index..end.min(bytes.len()) {
                    if bytes[position] != b'\n' {
                        masked[position] = b' ';
                    }
                }
                index = end;
            } else {
                index += 1;
            }
        } else if bytes[index] == b'"' {
            let mut end = index + 1;
            while end < bytes.len() {
                if bytes[end] == b'\\' {
                    end = (end + 2).min(bytes.len());
                } else if bytes[end] == b'"' {
                    end += 1;
                    break;
                } else {
                    end += 1;
                }
            }
            for position in index..end.min(bytes.len()) {
                if bytes[position] != b'\n' {
                    masked[position] = b' ';
                }
            }
            index = end;
        } else {
            index += 1;
        }
    }
    String::from_utf8(masked).expect("mask preserves UTF-8 bytes")
}

fn rust_string_literals(source: &str) -> Vec<String> {
    let bytes = source.as_bytes();
    let mut literals = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        let (content_start, hashes) = if bytes[index] == b'"' {
            (index + 1, None)
        } else if bytes[index] == b'r' {
            let mut quote = index + 1;
            while quote < bytes.len() && bytes[quote] == b'#' {
                quote += 1;
            }
            if quote < bytes.len() && bytes[quote] == b'"' {
                (quote + 1, Some(quote - index - 1))
            } else {
                index += 1;
                continue;
            }
        } else {
            index += 1;
            continue;
        };
        let mut end = content_start;
        if let Some(hashes) = hashes {
            while end < bytes.len() && !closes_raw_string(bytes, end, hashes) {
                end += 1;
            }
            literals.push(String::from_utf8_lossy(&bytes[content_start..end]).into_owned());
            index = (end + hashes + 1).min(bytes.len());
        } else {
            while end < bytes.len() && bytes[end] != b'"' {
                if bytes[end] == b'\\' {
                    end = (end + 2).min(bytes.len());
                } else {
                    end += 1;
                }
            }
            literals.push(String::from_utf8_lossy(&bytes[content_start..end]).into_owned());
            index = (end + 1).min(bytes.len());
        }
    }
    literals
}

fn rust_docker_prune_lines(source: &str) -> Vec<usize> {
    let masked = mask_rust_non_code(source);
    let lower = masked.to_ascii_lowercase();
    let mut lines = Vec::new();
    let mut search_start = 0;
    while let Some(relative) = lower[search_start..].find("command::new") {
        let command = search_start + relative;
        let Some(open_relative) = lower[command..].find('(') else {
            break;
        };
        let open = command + open_relative;
        let Some(close_relative) = lower[open..].find(')') else {
            break;
        };
        let close = open + close_relative;
        let statement_end = lower[close..]
            .find(';')
            .map_or(source.len(), |relative| close + relative);
        let command_literals = rust_string_literals(&source[open + 1..close]);
        let docker_command = if command_literals.is_empty() {
            true
        } else {
            command_literals
                .iter()
                .any(|literal| is_docker_command(literal))
        };
        let arguments = rust_string_literals(&source[close..statement_end]);
        let prune = arguments.iter().enumerate().any(|(index, argument)| {
            matches!(
                argument.to_ascii_lowercase().as_str(),
                "system" | "container" | "image" | "network" | "volume"
            ) && arguments[index + 1..]
                .iter()
                .any(|later| later.eq_ignore_ascii_case("prune"))
        });
        if docker_command && prune {
            lines.push(
                source[..command]
                    .bytes()
                    .filter(|byte| *byte == b'\n')
                    .count()
                    + 1,
            );
        }
        search_start = statement_end.saturating_add(1);
    }
    lines
}

fn scan_files(files: Vec<PathBuf>) -> Vec<String> {
    let mut violations = Vec::new();
    for path in files {
        if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("md" | "txt" | "rst")
        ) {
            continue;
        }
        let source = fs::read_to_string(&path).expect("read auditable UTF-8 source");
        if rust_source(&path) {
            let masked = mask_rust_non_code(&source);
            let compact = masked
                .to_ascii_lowercase()
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>();
            for operation in ["selectgpu(", "loadmodel(", "gpuqueue(", "modelplacement("] {
                if compact.contains(operation) {
                    let line_number = masked
                        .lines()
                        .position(|line| {
                            line.to_ascii_lowercase()
                                .chars()
                                .filter(|character| !character.is_whitespace())
                                .collect::<String>()
                                .contains(operation)
                        })
                        .map_or(1, |line| line + 1);
                    violations.push(format!(
                        "{}:{line_number} invokes {operation}",
                        path.display()
                    ));
                }
            }
            for line_number in rust_docker_prune_lines(&source) {
                violations.push(format!(
                    "{}:{line_number} constructs global Docker pruning",
                    path.display()
                ));
            }
            continue;
        }
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
            let tokens = shell_tokens(&lower);
            if shell_like(&path) && shell_invokes_docker_prune(&tokens) {
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
    assert_eq!(violations.len(), 12, "{violations:#?}");
    assert!(violations.iter().all(|violation| violation.contains(':')));
}

#[test]
fn forbidden_scanner_catches_url_prefixed_prune_and_attached_operators() {
    let fixture = repository_root()
        .join("crates/orchestrator-core/tests/fixtures/forbidden-scanner/forbidden");
    let mut files = Vec::new();
    collect_files(&fixture, &mut files);
    let violations = scan_files(files);

    for fixture_name in [
        "url-prefixed.sh",
        "url-prefixed.yml",
        "Dockerfile.url-prefixed",
        "attached-operators.sh",
    ] {
        assert!(
            violations
                .iter()
                .any(|violation| violation.contains(fixture_name)),
            "missing {fixture_name}: {violations:#?}"
        );
    }
}

#[test]
fn forbidden_scanner_ignores_comments_and_bounded_cleanup() {
    let fixture =
        repository_root().join("crates/orchestrator-core/tests/fixtures/forbidden-scanner/allowed");
    let mut files = Vec::new();
    collect_files(&fixture, &mut files);
    assert!(scan_files(files).is_empty());
}
