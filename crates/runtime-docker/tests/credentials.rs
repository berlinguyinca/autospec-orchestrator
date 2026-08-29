use chrono::{Duration, Utc};
use orchestrator_core::{Execution, ExecutionId};
use runtime_docker::LocalCredentialBroker;
use runtime_traits::CredentialBroker;
use std::{
    fs,
    sync::{Arc, Barrier},
    thread,
};
use tempfile::TempDir;

fn execution(id: &str) -> Execution {
    serde_json::from_value(serde_json::json!({
        "id": id,
        "role": "implementation",
        "state": "WORKER_ASSIGNED",
        "manifest": {
            "apiVersion": "autospec.dev/v1alpha1",
            "role": "implementation",
            "task": {"project_id": "project", "issue_id": "7"},
            "repository": {"repo": "owner/repository", "baseRef": "main", "branch": "autospec/task-7"},
            "agent": {"harness": "pi", "modelPolicy": {"provider": "inferweave"}},
            "runtime": {"type": "docker", "cpu": 1, "memoryMib": 512, "diskGib": 1},
            "persistence": "resumable"
        },
        "worker_id": "worker-1",
        "attempt_id": "attempt-1",
        "labels": {
            "execution_id": id,
            "worker_id": "worker-1",
            "repository": "owner/repository",
            "issue": "7"
        },
        "created_at": "2026-08-29T00:00:00Z",
        "updated_at": "2026-08-29T00:00:00Z"
    }))
    .unwrap()
}

fn execution_root(root: &TempDir, id: &str) -> std::path::PathBuf {
    let execution_root = root.path().join("executions").join(id);
    fs::create_dir_all(execution_root.join("credentials")).unwrap();
    execution_root.canonicalize().unwrap()
}

#[tokio::test]
async fn credentials_are_execution_scoped_short_lived_private_and_unpredictable() {
    let root = TempDir::new().unwrap();
    let left = execution("repo-7-impl-01");
    let right = execution("repo-7-review-01");
    let left_root = execution_root(&root, left.id.as_str());
    let right_root = execution_root(&root, right.id.as_str());
    let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();

    let before = Utc::now();
    let left_credentials = broker.mint(&left).await.unwrap();
    let right_credentials = broker.mint(&right).await.unwrap();
    let after = Utc::now();

    assert!(left_credentials
        .path
        .starts_with(left_root.join("credentials")));
    assert!(right_credentials
        .path
        .starts_with(right_root.join("credentials")));
    assert_ne!(left_credentials.path, right_credentials.path);
    assert_ne!(
        fs::read(&left_credentials.path).unwrap(),
        fs::read(&right_credentials.path).unwrap()
    );
    assert!(left_credentials.expires_at > before);
    assert!(left_credentials.expires_at <= after + Duration::minutes(5));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&left_credentials.path)
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn revoke_is_idempotent_and_cannot_remove_a_peer_credential() {
    let root = TempDir::new().unwrap();
    let left = execution("repo-7-impl-01");
    let right = execution("repo-7-review-01");
    execution_root(&root, left.id.as_str());
    execution_root(&root, right.id.as_str());
    let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();
    let left_credentials = broker.mint(&left).await.unwrap();
    let right_credentials = broker.mint(&right).await.unwrap();

    broker.revoke(&left.id).await.unwrap();
    broker.revoke(&left.id).await.unwrap();

    assert!(!left_credentials.path.exists());
    assert!(right_credentials.path.exists());
    assert!(broker.revoke(&ExecutionId::new("../peer")).await.is_err());
    assert!(right_credentials.path.exists());
}

#[tokio::test]
async fn revoke_remains_idempotent_after_execution_storage_is_already_absent() {
    let root = TempDir::new().unwrap();
    let execution = execution("repo-7-recovery-01");
    let execution_root = execution_root(&root, execution.id.as_str());
    let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();
    broker.mint(&execution).await.unwrap();
    fs::remove_dir_all(execution_root).unwrap();

    broker.revoke(&execution.id).await.unwrap();
    broker.revoke(&execution.id).await.unwrap();
}

#[test]
fn concurrent_mint_reuses_one_atomic_winner() {
    let root = TempDir::new().unwrap();
    let execution = execution("repo-7-race-01");
    execution_root(&root, execution.id.as_str());
    let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();
    let start = Arc::new(Barrier::new(3));
    let (left, right) = thread::scope(|scope| {
        let left_start = start.clone();
        let left_broker = broker.clone();
        let left_execution = execution.clone();
        let left = scope.spawn(move || {
            left_start.wait();
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(left_broker.mint(&left_execution))
        });
        let right_start = start.clone();
        let right_broker = broker.clone();
        let right_execution = execution.clone();
        let right = scope.spawn(move || {
            right_start.wait();
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(right_broker.mint(&right_execution))
        });
        start.wait();
        (
            left.join().unwrap().unwrap(),
            right.join().unwrap().unwrap(),
        )
    });
    assert_eq!(left.path, right.path);
    assert_eq!(left.expires_at, right.expires_at);
    assert_eq!(
        fs::read(&left.path).unwrap(),
        fs::read(&right.path).unwrap()
    );
    let names = fs::read_dir(left.path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["inferweave.credential"]);
}

#[test]
fn concurrent_mint_and_revoke_are_linearized_per_execution() {
    let root = TempDir::new().unwrap();
    let execution = execution("repo-7-mint-revoke-race-01");
    execution_root(&root, execution.id.as_str());
    let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(broker.mint(&execution)).unwrap();
    let start = Arc::new(Barrier::new(3));
    thread::scope(|scope| {
        let mint_start = start.clone();
        let mint_broker = broker.clone();
        let mint_execution = execution.clone();
        let mint = scope.spawn(move || {
            mint_start.wait();
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(mint_broker.mint(&mint_execution))
        });
        let revoke_start = start.clone();
        let revoke_broker = broker.clone();
        let revoke_id = execution.id.clone();
        let revoke = scope.spawn(move || {
            revoke_start.wait();
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(revoke_broker.revoke(&revoke_id))
        });
        start.wait();
        mint.join().unwrap().unwrap();
        revoke.join().unwrap().unwrap();
    });

    let reminted = runtime.block_on(broker.mint(&execution)).unwrap();
    let body = fs::read_to_string(&reminted.path).unwrap();
    assert_eq!(body.lines().count(), 2);
    let names = fs::read_dir(reminted.path.parent().unwrap())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(names, vec!["inferweave.credential"]);
}

#[tokio::test]
async fn expired_bound_credential_is_rejected_without_replacing_its_inode() {
    let root = TempDir::new().unwrap();
    let execution = execution("repo-7-expired-01");
    let execution_root = execution_root(&root, execution.id.as_str());
    let credential = execution_root.join("credentials/inferweave.credential");
    fs::write(&credential, "expired\n2020-01-01T00:00:00Z\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        fs::set_permissions(&credential, fs::Permissions::from_mode(0o600)).unwrap();
        let inode = fs::metadata(&credential).unwrap().ino();
        let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();
        assert!(broker.mint(&execution).await.is_err());
        assert_eq!(fs::metadata(&credential).unwrap().ino(), inode);
        assert_eq!(
            fs::read_to_string(&credential).unwrap(),
            "expired\n2020-01-01T00:00:00Z\n"
        );
    }
}

#[tokio::test]
async fn revoke_removes_exact_malformed_file_but_rejects_symlink_authority() {
    let root = TempDir::new().unwrap();
    let malformed = execution("repo-7-malformed-01");
    let malformed_root = execution_root(&root, malformed.id.as_str());
    let malformed_path = malformed_root.join("credentials/inferweave.credential");
    fs::write(&malformed_path, "not-a-credential").unwrap();
    let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();
    broker.revoke(&malformed.id).await.unwrap();
    assert!(!malformed_path.exists());

    #[cfg(unix)]
    {
        let symlinked = execution("repo-7-symlink-01");
        let symlinked_root = execution_root(&root, symlinked.id.as_str());
        let target = root.path().join("foreign-secret");
        fs::write(&target, "preserve").unwrap();
        std::os::unix::fs::symlink(
            &target,
            symlinked_root.join("credentials/inferweave.credential"),
        )
        .unwrap();
        assert!(broker.revoke(&symlinked.id).await.is_err());
        assert_eq!(fs::read_to_string(target).unwrap(), "preserve");
    }
}

#[tokio::test]
async fn mint_and_revoke_scavenge_only_exact_validated_crash_candidates() {
    let root = TempDir::new().unwrap();
    let execution = execution("repo-7-candidate-recovery-01");
    let execution_root = execution_root(&root, execution.id.as_str());
    let credentials = execution_root.join("credentials");
    let crashed = credentials.join(format!(".inferweave.credential.{}.tmp", "a".repeat(64)));
    let unrelated = credentials.join(".inferweave.credential.not-hex.tmp");
    fs::write(&crashed, "orphan\n2026-08-29T00:00:00Z\n").unwrap();
    fs::write(&unrelated, "preserve").unwrap();
    let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();

    let minted = broker.mint(&execution).await.unwrap();
    assert!(!crashed.exists());
    assert!(unrelated.exists());
    assert!(minted.path.exists());

    let second_crash = credentials.join(format!(".inferweave.credential.{}.tmp", "b".repeat(64)));
    fs::write(&second_crash, "orphan\n2026-08-29T00:00:00Z\n").unwrap();
    broker.revoke(&execution.id).await.unwrap();
    assert!(!second_crash.exists());
    assert!(!minted.path.exists());
    assert!(unrelated.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn candidate_scavenging_fails_closed_on_symlinks_and_irregular_files() {
    let root = TempDir::new().unwrap();
    let execution = execution("repo-7-candidate-adversarial-01");
    let execution_root = execution_root(&root, execution.id.as_str());
    let credentials = execution_root.join("credentials");
    let target = root.path().join("foreign-candidate-target");
    fs::write(&target, "preserve").unwrap();
    let candidate = credentials.join(format!(".inferweave.credential.{}.tmp", "c".repeat(64)));
    std::os::unix::fs::symlink(&target, &candidate).unwrap();
    let broker = LocalCredentialBroker::new(root.path(), Duration::minutes(5)).unwrap();

    assert!(broker.mint(&execution).await.is_err());
    assert!(candidate.is_symlink());
    assert_eq!(fs::read_to_string(&target).unwrap(), "preserve");
    assert!(broker.revoke(&execution.id).await.is_err());
    assert!(candidate.is_symlink());
    assert_eq!(fs::read_to_string(target).unwrap(), "preserve");
}
