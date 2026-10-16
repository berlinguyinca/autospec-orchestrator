//! The controller binary's worker credential startup rules, end to end:
//! CLI/env -> `ServerConfig` -> `serve` -> the router that answers workers.

use sha2::{Digest, Sha256};
use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    os::unix::fs::PermissionsExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

fn digest_hex(token: &str) -> String {
    Sha256::digest(token.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn controller(database_url: &str, addr: &str) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_autospec-orchestrator"));
    command
        .env_clear()
        .env("AUTOSPEC_DATABASE_URL", database_url)
        .env("AUTOSPEC_API_TOKEN", "api-secret")
        .env("AUTOSPEC_ORCHESTRATOR_ADDR", addr)
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command
}

fn tokens_file(dir: &Path, mode: u32, contents: &str) -> std::path::PathBuf {
    let path = dir.join("worker-tokens");
    std::fs::write(&path, contents).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
    path
}

/// Runs the binary expecting it to exit by itself, returning stderr.
fn refused(mut command: Command) -> String {
    let mut child = command.spawn().expect("spawn controller");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success(), "controller must refuse to start");
            let mut stderr = String::new();
            child
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            return stderr;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("controller neither exited nor failed within 30s");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn startup_refuses_without_a_credential_source_and_names_the_flags() {
    // An unreachable database proves the refusal happens before any connection.
    let stderr = refused(controller(
        "postgres://nobody@127.0.0.1:1/none",
        "127.0.0.1:0",
    ));
    for flag in ["--worker-tokens-file", "--allow-shared-worker-token"] {
        assert!(stderr.contains(flag), "{stderr}");
    }
}

#[test]
fn startup_refuses_the_shared_token_without_the_explicit_flag() {
    let mut command = controller("postgres://nobody@127.0.0.1:1/none", "127.0.0.1:0");
    command.env("AUTOSPEC_WORKER_TOKEN", "shared-secret");
    let stderr = refused(command);
    assert!(stderr.contains("--allow-shared-worker-token"), "{stderr}");
    assert!(!stderr.contains("shared-secret"), "{stderr}");
}

#[test]
fn startup_refuses_a_group_readable_tokens_file_and_both_sources() {
    let dir = tempfile::tempdir().unwrap();
    let line = format!("w1 {}\n", digest_hex("w1-token"));
    let open = tokens_file(dir.path(), 0o644, &line);
    let mut command = controller("postgres://nobody@127.0.0.1:1/none", "127.0.0.1:0");
    command.env("AUTOSPEC_WORKER_TOKENS_FILE", &open);
    let stderr = refused(command);
    assert!(stderr.contains("chmod 600"), "{stderr}");
    assert!(!stderr.contains(&digest_hex("w1-token")), "{stderr}");

    let private = tokens_file(dir.path(), 0o600, &line);
    let mut command = controller("postgres://nobody@127.0.0.1:1/none", "127.0.0.1:0");
    command
        .env("AUTOSPEC_WORKER_TOKENS_FILE", &private)
        .env("AUTOSPEC_WORKER_TOKEN", "shared-secret");
    let stderr = refused(command);
    assert!(stderr.contains("mutually exclusive"), "{stderr}");
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn register_status(port: u16, token: &str, worker: &str) -> u16 {
    let body = format!(
        r#"{{"id":"{worker}","capabilities":{{"os":"linux","arch":"x86_64","cpu":4,"memoryMib":4096,"diskGib":40,"runtimes":["docker"],"capabilities":[],"toolchains":{{}},"maxConcurrentExecutions":1,"healthErrors":[]}}}}"#
    );
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .unwrap();
    write!(
        stream,
        "POST /api/v1/workers HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    response
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or_else(|| panic!("no status in {response:?}"))
}

fn wait_listening(child: &mut Child, port: u16) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while Instant::now() < deadline {
        assert!(
            child.try_wait().unwrap().is_none(),
            "controller exited early"
        );
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    panic!("controller never listened");
}

struct Kill(Child);
impl Drop for Kill {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn a_running_controller_authenticates_workers_by_the_configured_source() {
    let Ok(database_url) = std::env::var("AUTOSPEC_DATABASE_URL") else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required to run the controller");
        return;
    };
    let nonce = format!("{:x}", std::process::id());
    let (w1, w2) = (format!("cred-w1-{nonce}"), format!("cred-w2-{nonce}"));
    let dir = tempfile::tempdir().unwrap();
    let file = tokens_file(
        dir.path(),
        0o600,
        &format!(
            "# per-worker\n{w1} {}\n{w2} {}\n",
            digest_hex("tok-one"),
            digest_hex("tok-two")
        ),
    );

    // Per-worker file: each token authenticates; the shared token does not.
    let port = free_port();
    let mut command = controller(&database_url, &format!("127.0.0.1:{port}"));
    command
        .env("AUTOSPEC_WORKER_TOKENS_FILE", &file)
        .env("AUTOSPEC_STATE_ROOT", dir.path());
    let mut server = Kill(command.spawn().unwrap());
    wait_listening(&mut server.0, port);
    assert_eq!(register_status(port, "tok-one", &w1), 201);
    // Binding a token to its own worker is enforced by the routes (issue #78).
    assert_eq!(register_status(port, "tok-two", &w2), 201);
    assert_eq!(register_status(port, "shared-secret", &w1), 401);
    drop(server);

    // Explicit opt-in: the shared token speaks for every worker.
    let port = free_port();
    let mut command = controller(&database_url, &format!("127.0.0.1:{port}"));
    command
        .env("AUTOSPEC_WORKER_TOKEN", "shared-secret")
        .env("AUTOSPEC_ALLOW_SHARED_WORKER_TOKEN", "true")
        .env("AUTOSPEC_STATE_ROOT", dir.path());
    let mut server = Kill(command.spawn().unwrap());
    wait_listening(&mut server.0, port);
    assert_eq!(register_status(port, "shared-secret", &w2), 200);
    assert_eq!(register_status(port, "tok-one", &w1), 401);
}
