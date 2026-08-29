use chrono::Utc;
use git_worktree::DiffCapture;
use orchestrator_core::{
    AgentAssignment, AttemptId, Execution, ExecutionId, ExecutionManifest, ExecutionState,
    HarnessKind, ModelPolicy, OwnershipLabels, PersistenceMode, RepositoryReference, Role,
    RuntimeRequirement, WorkerId,
};
use orchestrator_persistence::{ArtifactStore, ExecutionStore, PgArtifactStore, PgExecutionStore};
use orchestrator_worker::{ContentAddressedEvidenceStore, EvidenceStore};
use std::sync::Arc;

#[tokio::test]
async fn production_evidence_adapter_persists_metadata_without_touching_the_pi_manifest() {
    let Some(database_url) = std::env::var("AUTOSPEC_DATABASE_URL").ok() else {
        eprintln!("SKIP: AUTOSPEC_DATABASE_URL is required for real artifact evidence test");
        return;
    };
    let suffix = format!(
        "{}-{}",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    );
    let root = std::env::temp_dir().join(format!("autospec-evidence-{suffix}"));
    std::fs::create_dir(&root).unwrap();
    let executions = PgExecutionStore::connect(&database_url).await.unwrap();
    let artifacts = Arc::new(
        PgArtifactStore::connect(&database_url, &root)
            .await
            .unwrap(),
    );
    let id = ExecutionId::new(format!("evidence-{suffix}"));
    let attempt_id = AttemptId::new(format!("attempt-{suffix}"));
    let manifest = manifest();
    let manifest_before = serde_json::to_value(&manifest).unwrap();
    let now = Utc::now();
    let execution = Execution {
        id: id.clone(),
        role: Role::Implementation,
        state: ExecutionState::Running,
        manifest: manifest.clone(),
        worker_id: Some(WorkerId::new("worker-evidence")),
        attempt_id: Some(attempt_id.clone()),
        session_id: None,
        worktree_path: None,
        labels: OwnershipLabels {
            execution_id: id.clone(),
            worker_id: WorkerId::new("worker-evidence"),
            repository: "InferWeave/inferweave-node".into(),
            issue: Some("417".into()),
        },
        created_at: now,
        updated_at: now,
        result: None,
    };
    executions.insert(&execution).await.unwrap();
    let evidence = ContentAddressedEvidenceStore::new(artifacts.clone());
    let patch = "diff --git a/a b/a\n+verified\n";

    let artifact_id = evidence
        .persist(
            &execution,
            &DiffCapture {
                patch: patch.into(),
                changed_files: vec!["a".into()],
            },
        )
        .await
        .unwrap();

    let listed = artifacts.list(&id).await.unwrap();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].sha256, artifact_id);
    assert_eq!(listed[0].name, format!("diff-{attempt_id}.patch"));
    assert_eq!(listed[0].size_bytes, patch.len() as u64);
    assert_eq!(
        serde_json::to_value(&execution.manifest).unwrap(),
        manifest_before
    );
    assert!(!serde_json::to_string(&execution.manifest)
        .unwrap()
        .contains("diff --git"));
    std::fs::remove_dir_all(root).unwrap();
}

fn manifest() -> ExecutionManifest {
    ExecutionManifest {
        api_version: orchestrator_core::MANIFEST_API_VERSION.into(),
        role: Role::Implementation,
        task: None,
        repository: RepositoryReference {
            repo: "InferWeave/inferweave-node".into(),
            base_ref: "main".into(),
            base_sha: None,
            branch: Some("autospec/417".into()),
        },
        agent: AgentAssignment {
            harness: HarnessKind::Pi,
            model_policy: ModelPolicy {
                provider: "inferweave".into(),
                preferred: Vec::new(),
                alternatives: Vec::new(),
                fallback_class: Some("coding-high".into()),
            },
        },
        runtime: RuntimeRequirement::default(),
        services: Vec::new(),
        persistence: PersistenceMode::Resumable,
        task_packet: None,
    }
}
