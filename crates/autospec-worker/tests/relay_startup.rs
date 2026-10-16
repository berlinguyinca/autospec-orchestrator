//! The worker binary's inference-relay startup rule, end to end: the real
//! process exits before touching storage, Docker or the database.

use std::process::{Command, Output};

fn worker(extra: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_autospec-worker"))
        .env_clear()
        .args([
            "--worker-id",
            "w",
            "--worker-token",
            "throwaway",
            "--storage-kind",
            "apfs",
            "--storage-pool",
            "/tmp",
            "--docker-verifier-image",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--database-url",
            "postgres://nobody@127.0.0.1:1/none",
        ])
        .args(extra)
        .output()
        .expect("run autospec-worker")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn a_worker_with_no_relay_and_no_egress_opt_out_refuses_to_start_naming_the_flags() {
    let output = worker(&[]);
    assert!(!output.status.success());
    let stderr = stderr(&output);
    for flag in [
        "--inference-upstream",
        "--inference-relay-image",
        "--allow-agent-egress",
    ] {
        assert!(stderr.contains(flag), "{stderr}");
    }
    assert!(
        !stderr.contains("throwaway"),
        "the token must not be printed"
    );
}

#[test]
fn the_environment_twins_are_read_and_a_half_configured_relay_is_refused() {
    let output = Command::new(env!("CARGO_BIN_EXE_autospec-worker"))
        .env_clear()
        .env("AUTOSPEC_INFERENCE_UPSTREAM", "gateway.local:8080")
        .args([
            "--worker-id",
            "w",
            "--worker-token",
            "throwaway",
            "--storage-kind",
            "apfs",
            "--storage-pool",
            "/tmp",
            "--docker-verifier-image",
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "--database-url",
            "postgres://nobody@127.0.0.1:1/none",
        ])
        .output()
        .expect("run autospec-worker");
    assert!(!output.status.success());
    let stderr = stderr(&output);
    assert!(
        stderr.contains("must be given together"),
        "the env twin AUTOSPEC_INFERENCE_UPSTREAM reached the rule: {stderr}"
    );
}

#[test]
fn a_malformed_upstream_is_refused_before_any_work() {
    let output = worker(&[
        "--inference-upstream",
        "http://gateway.local:8080",
        "--inference-relay-image",
        "relay:1",
    ]);
    assert!(!output.status.success());
    assert!(stderr(&output).contains("host:port"), "{}", stderr(&output));
}
