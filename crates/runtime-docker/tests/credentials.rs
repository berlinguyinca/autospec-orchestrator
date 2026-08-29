use chrono::{Duration, Utc};
use orchestrator_core::{Execution, ExecutionId};
use runtime_docker::LocalCredentialBroker;
use runtime_traits::CredentialBroker;
use std::fs;
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
