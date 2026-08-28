use git_worktree::{GitWorktreeManager, WorktreeError, WorktreeManager};
use orchestrator_core::{ExecutionId, OwnershipLabels, WorkerId};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

struct TestRepository {
    _temp: TempDir,
    path: PathBuf,
}

impl TestRepository {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("create repository tempdir");
        let path = temp.path().join("owner").join("project.git");
        std::fs::create_dir_all(path.parent().expect("repository parent"))
            .expect("create repository parent");
        git(
            temp.path(),
            &["init", path.to_str().expect("utf-8 repository path")],
        );
        git(&path, &["config", "user.email", "tests@example.com"]);
        git(&path, &["config", "user.name", "Autospec Tests"]);
        std::fs::write(path.join("README.md"), "first\n").expect("write initial file");
        git(&path, &["add", "README.md"]);
        git(&path, &["commit", "-m", "initial"]);
        Self { _temp: temp, path }
    }

    fn commit(&self, contents: &str) -> String {
        std::fs::write(self.path.join("README.md"), contents).expect("update repository file");
        git(&self.path, &["add", "README.md"]);
        git(&self.path, &["commit", "-m", "update"]);
        git_output(&self.path, &["rev-parse", "HEAD"])
    }
}

fn git(current_dir: &Path, args: &[&str]) {
    let output = Command::new("git")
        .current_dir(current_dir)
        .args(args)
        .output()
        .expect("run git command");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_output(current_dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(current_dir)
        .args(args)
        .output()
        .expect("run git command");
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git output is utf-8")
        .trim()
        .to_owned()
}

fn git_succeeds(current_dir: &Path, args: &[&str]) -> bool {
    Command::new("git")
        .current_dir(current_dir)
        .args(args)
        .output()
        .expect("run git command")
        .status
        .success()
}

fn labels(execution_id: &str, repository: &Path) -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new(execution_id),
        worker_id: WorkerId::new("worker-01"),
        repository: repository.to_string_lossy().into_owned(),
        issue: Some("11".to_owned()),
    }
}

#[test]
fn mirror_uses_safe_owner_repo_name_and_refreshes() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());

    let mirror = PathBuf::from(
        manager
            .ensure_mirror(repository.path.to_str().expect("utf-8 repository path"))
            .expect("create mirror"),
    );
    assert_eq!(
        mirror.file_name().and_then(|name| name.to_str()),
        Some("owner__project.git")
    );
    assert_eq!(
        mirror.parent(),
        Some(state.path().join("mirrors").as_path())
    );

    let latest = repository.commit("second\n");
    manager
        .ensure_mirror(repository.path.to_str().expect("utf-8 repository path"))
        .expect("refresh mirror");
    assert_eq!(git_output(&mirror, &["rev-parse", "HEAD"]), latest);
}

#[test]
fn create_places_owned_worktree_under_execution_id() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let labels = labels("project-11-impl-01", &repository.path);
    let base_sha = git_output(&repository.path, &["rev-parse", "HEAD"]);

    let worktree = manager
        .create(
            &labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/project-11-impl-01",
        )
        .expect("create worktree");

    assert_eq!(
        Path::new(&worktree.path),
        state.path().join("worktrees/project-11-impl-01")
    );
    assert_eq!(worktree.base_sha, base_sha);
    assert_eq!(
        git_output(Path::new(&worktree.path), &["branch", "--show-current"]),
        "autospec/project-11-impl-01"
    );

    let owner: Value = serde_json::from_slice(
        &std::fs::read(Path::new(&worktree.path).join(".autospec-owner.json"))
            .expect("read owner record"),
    )
    .expect("parse owner record");
    assert_eq!(owner["base_sha"], base_sha);
    assert_eq!(owner["labels"]["autospec.managed"], "true");
    assert_eq!(
        owner["labels"]["autospec.execution_id"],
        "project-11-impl-01"
    );
}

#[test]
fn create_rejects_execution_id_path_traversal() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let labels = labels("../escape", &repository.path);

    let error = manager
        .create(
            &labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/escape",
        )
        .expect_err("reject path traversal");

    assert!(matches!(error, WorktreeError::Create(message) if message.contains("execution_id")));
    assert!(!state.path().join("escape").exists());
}

#[test]
fn create_rejects_empty_execution_id() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let labels = labels("", &repository.path);

    let error = manager
        .create(
            &labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/empty",
        )
        .expect_err("reject empty execution id");

    assert!(matches!(error, WorktreeError::Create(message) if message.contains("execution_id")));
}

#[test]
fn create_rejects_repository_label_mismatch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let mut labels = labels("project-12-impl-01", &repository.path);
    labels.repository = "different/target".to_owned();

    let error = manager
        .create(
            &labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/project-12-impl-01",
        )
        .expect_err("reject mismatched repository ownership");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(!state.path().join("worktrees/project-12-impl-01").exists());
}

#[test]
fn create_reports_execution_that_owns_checked_out_branch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let first_labels = labels("project-11-impl-01", &repository.path);
    manager
        .create(
            &first_labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/shared-branch",
        )
        .expect("create first worktree");

    let second_labels = labels("project-11-review-01", &repository.path);
    let error = manager
        .create(
            &second_labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/shared-branch",
        )
        .expect_err("reject branch collision");

    assert!(matches!(
        error,
        WorktreeError::Locked(execution_id)
            if execution_id == ExecutionId::new("project-11-impl-01")
    ));
}

#[test]
fn capture_diff_includes_patch_and_all_changed_files() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let labels = labels("project-13-impl-01", &repository.path);
    let worktree = manager
        .create(
            &labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/project-13-impl-01",
        )
        .expect("create worktree");
    let worktree_path = Path::new(&worktree.path);
    std::fs::write(worktree_path.join("README.md"), "modified\n").expect("modify tracked file");
    std::fs::write(worktree_path.join("evidence.txt"), "untracked evidence\n")
        .expect("write untracked file");

    let capture = manager.capture_diff(&worktree).expect("capture diff");

    assert_eq!(capture.changed_files, vec!["README.md", "evidence.txt"]);
    assert!(capture.patch.contains("+modified"));
    assert!(capture.patch.contains("+untracked evidence"));
}

#[test]
fn capture_diff_refuses_owner_record_outside_execution_path() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let labels = labels("project-13-impl-02", &repository.path);
    let worktree = manager
        .create(
            &labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/project-13-impl-02",
        )
        .expect("create worktree");
    let forged_owner = std::fs::read(Path::new(&worktree.path).join(".autospec-owner.json"))
        .expect("read owner record");
    std::fs::write(repository.path.join(".autospec-owner.json"), forged_owner)
        .expect("write forged owner record");
    let mut forged = worktree;
    forged.path = repository.path.to_string_lossy().into_owned();

    let error = manager
        .capture_diff(&forged)
        .expect_err("reject owner record outside state root");

    assert!(matches!(error, WorktreeError::Ownership(_)));
}

#[test]
fn destroy_removes_only_the_owned_worktree_and_branch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let repository_path = repository.path.to_str().expect("utf-8 repository path");
    let mirror = manager
        .ensure_mirror(repository_path)
        .expect("create mirror");
    let labels = labels("project-14-impl-01", &repository.path);
    let worktree = manager
        .create(
            &labels,
            repository_path,
            "HEAD",
            "autospec/project-14-impl-01",
        )
        .expect("create worktree");

    manager.destroy(&worktree).expect("destroy worktree");

    assert!(!Path::new(&worktree.path).exists());
    assert!(!git_succeeds(
        state.path(),
        &[
            "--git-dir",
            &mirror,
            "show-ref",
            "--verify",
            "refs/heads/autospec/project-14-impl-01",
        ],
    ));
}

#[test]
fn destroy_refuses_tampered_owner_record() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let labels = labels("project-14-impl-02", &repository.path);
    let worktree = manager
        .create(
            &labels,
            repository.path.to_str().expect("utf-8 repository path"),
            "HEAD",
            "autospec/project-14-impl-02",
        )
        .expect("create worktree");
    let owner_path = Path::new(&worktree.path).join(".autospec-owner.json");
    let mut owner: Value =
        serde_json::from_slice(&std::fs::read(&owner_path).expect("read owner record"))
            .expect("parse owner record");
    owner["labels"]["autospec.execution_id"] = Value::String("someone-else".to_owned());
    std::fs::write(
        &owner_path,
        serde_json::to_vec_pretty(&owner).expect("serialize owner record"),
    )
    .expect("tamper owner record");

    let error = manager
        .destroy(&worktree)
        .expect_err("refuse unowned worktree");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(Path::new(&worktree.path).exists());
}

#[test]
fn find_stale_returns_owned_worktrees_not_in_live_set() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::new(state.path());
    let repository_path = repository.path.to_str().expect("utf-8 repository path");
    let live_labels = labels("project-14-impl-03", &repository.path);
    manager
        .create(
            &live_labels,
            repository_path,
            "HEAD",
            "autospec/project-14-impl-03",
        )
        .expect("create live worktree");
    let stale_labels = labels("project-14-review-01", &repository.path);
    let stale_worktree = manager
        .create(
            &stale_labels,
            repository_path,
            "HEAD",
            "autospec/project-14-review-01",
        )
        .expect("create stale worktree");

    let stale = manager
        .find_stale(&[live_labels.execution_id])
        .expect("find stale worktrees");

    assert_eq!(stale.len(), 1);
    assert_eq!(stale[0].execution_id, stale_labels.execution_id);
    assert_eq!(stale[0].path, stale_worktree.path);
    assert_eq!(stale[0].branch, stale_worktree.branch);
    assert_eq!(stale[0].base_sha, stale_worktree.base_sha);
}
