use git_worktree::{GitWorktreeManager, WorktreeError, WorktreeManager};
use orchestrator_core::{ExecutionId, OwnershipLabels, WorkerId};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;

struct TestRepository {
    _temp: TempDir,
    path: PathBuf,
    clone_base: PathBuf,
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
        let clone_base = temp.path().to_path_buf();
        Self {
            _temp: temp,
            path,
            clone_base,
        }
    }

    fn commit(&self, contents: &str) -> String {
        std::fs::write(self.path.join("README.md"), contents).expect("update repository file");
        git(&self.path, &["add", "README.md"]);
        git(&self.path, &["commit", "-m", "update"]);
        git_output(&self.path, &["rev-parse", "HEAD"])
    }

    fn canonical(&self) -> &'static str {
        "owner/project"
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

fn manager(state: &TempDir, repository: &TestRepository) -> GitWorktreeManager {
    GitWorktreeManager::with_clone_base(
        state.path(),
        repository.clone_base.to_str().expect("utf-8 clone base"),
    )
}

fn labels(execution_id: &str, repository: &str) -> OwnershipLabels {
    OwnershipLabels {
        execution_id: ExecutionId::new(execution_id),
        worker_id: WorkerId::new("worker-01"),
        repository: repository.to_owned(),
        issue: Some("11".to_owned()),
    }
}

#[test]
fn mirror_uses_safe_owner_repo_name_and_refreshes() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);

    let mirror = PathBuf::from(
        manager
            .ensure_mirror(repository.canonical())
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
        .ensure_mirror(repository.canonical())
        .expect("refresh mirror");
    assert_eq!(git_output(&mirror, &["rev-parse", "HEAD"]), latest);
}

#[test]
fn mirror_refuses_mismatched_existing_origin_without_fetching() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let mirror = manager
        .ensure_mirror(repository.canonical())
        .expect("create mirror");
    let original_head = git_output(Path::new(&mirror), &["rev-parse", "HEAD"]);
    let original_refs = git_output(
        Path::new(&mirror),
        &["for-each-ref", "--format=%(refname):%(objectname)"],
    );

    let impostor = repository.clone_base.join("impostor.git");
    git(
        repository.clone_base.as_path(),
        &["init", impostor.to_str().expect("utf-8 impostor path")],
    );
    git(&impostor, &["config", "user.email", "tests@example.com"]);
    git(&impostor, &["config", "user.name", "Autospec Tests"]);
    std::fs::write(impostor.join("IMPOSTOR.md"), "wrong source\n").expect("write impostor file");
    git(&impostor, &["add", "IMPOSTOR.md"]);
    git(&impostor, &["commit", "-m", "impostor"]);
    git(
        Path::new(&mirror),
        &[
            "remote",
            "set-url",
            "origin",
            impostor.to_str().expect("utf-8 impostor path"),
        ],
    );

    let error = manager
        .ensure_mirror(repository.canonical())
        .expect_err("reject substituted mirror origin");

    assert!(matches!(error, WorktreeError::Mirror(_)));
    assert_eq!(
        git_output(Path::new(&mirror), &["rev-parse", "HEAD"]),
        original_head
    );
    assert_eq!(
        git_output(
            Path::new(&mirror),
            &["for-each-ref", "--format=%(refname):%(objectname)"],
        ),
        original_refs
    );
}

#[test]
fn mirror_allows_single_underscores_in_canonical_components() {
    let mut repository = TestRepository::new();
    let underscored = repository
        .clone_base
        .join("single_owner/single_project.git");
    std::fs::create_dir_all(underscored.parent().expect("underscored repository parent"))
        .expect("create underscored repository parent");
    std::fs::rename(&repository.path, &underscored).expect("move repository");
    repository.path = underscored;
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);

    let mirror = manager
        .ensure_mirror("single_owner/single_project")
        .expect("mirror canonical identity with single underscores");

    assert_eq!(
        Path::new(&mirror)
            .file_name()
            .and_then(|name| name.to_str()),
        Some("single_owner__single_project.git")
    );
}

#[test]
fn create_places_owned_worktree_under_execution_id() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-11-impl-01", repository.canonical());
    let base_sha = git_output(&repository.path, &["rev-parse", "HEAD"]);

    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-01",
        )
        .expect("create worktree");

    assert_eq!(
        Path::new(&worktree.path),
        state.path().join("worktrees/project-11-impl-01")
    );
    assert_eq!(worktree.base_sha, base_sha);
    assert_eq!(worktree.repository, repository.canonical());
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
    assert_eq!(owner["branch"], "autospec/project-11-impl-01");
    assert_eq!(owner["labels"]["autospec.managed"], "true");
    assert_eq!(
        owner["labels"]["autospec.execution_id"],
        "project-11-impl-01"
    );
}

#[cfg(unix)]
#[test]
fn create_rejects_committed_owner_symlinks_without_touching_targets() {
    use std::os::unix::fs::symlink;

    let repository = TestRepository::new();
    let external_final = repository.clone_base.join("external-final.json");
    let external_temp = repository.clone_base.join("external-temp.json");
    std::fs::write(&external_final, "final-safe\n").expect("write final target");
    std::fs::write(&external_temp, "temp-safe\n").expect("write temp target");
    symlink(
        &external_final,
        repository.path.join(".autospec-owner.json"),
    )
    .expect("create committed final symlink");
    symlink(
        &external_temp,
        repository.path.join(".autospec-owner.json.tmp"),
    )
    .expect("create committed temp symlink");
    git(
        &repository.path,
        &["add", ".autospec-owner.json", ".autospec-owner.json.tmp"],
    );
    git(&repository.path, &["commit", "-m", "malicious metadata"]);

    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let mirror = manager
        .ensure_mirror(repository.canonical())
        .expect("create mirror");
    let labels = labels("project-12-impl-02", repository.canonical());

    let error = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-12-impl-02",
        )
        .expect_err("reject pre-existing metadata paths");

    assert!(matches!(error, WorktreeError::Create(_)));
    assert_eq!(
        std::fs::read_to_string(&external_final).expect("read final target"),
        "final-safe\n"
    );
    assert_eq!(
        std::fs::read_to_string(&external_temp).expect("read temp target"),
        "temp-safe\n"
    );
    assert!(!state.path().join("worktrees/project-12-impl-02").exists());
    assert!(!git_succeeds(
        state.path(),
        &[
            "--git-dir",
            &mirror,
            "show-ref",
            "--verify",
            "refs/heads/autospec/project-12-impl-02",
        ],
    ));
}

#[cfg(unix)]
#[test]
fn create_rejects_committed_owner_temp_symlink_without_touching_target() {
    use std::os::unix::fs::symlink;

    let repository = TestRepository::new();
    let external = repository.clone_base.join("external-temp-only.json");
    std::fs::write(&external, "still-safe\n").expect("write external target");
    symlink(&external, repository.path.join(".autospec-owner.json.tmp"))
        .expect("create committed temp symlink");
    git(&repository.path, &["add", ".autospec-owner.json.tmp"]);
    git(
        &repository.path,
        &["commit", "-m", "malicious temp metadata"],
    );

    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-12-impl-03", repository.canonical());

    manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-12-impl-03",
        )
        .expect_err("reject pre-existing temp metadata path");

    assert_eq!(
        std::fs::read_to_string(external).expect("read external target"),
        "still-safe\n"
    );
    assert!(!state.path().join("worktrees/project-12-impl-03").exists());
}

#[test]
fn create_rejects_execution_id_path_traversal() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("../escape", repository.canonical());

    let error = manager
        .create(&labels, repository.canonical(), "HEAD", "autospec/escape")
        .expect_err("reject path traversal");

    assert!(matches!(error, WorktreeError::Create(message) if message.contains("execution_id")));
    assert!(!state.path().join("escape").exists());
}

#[test]
fn create_rejects_empty_execution_id() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("", repository.canonical());

    let error = manager
        .create(&labels, repository.canonical(), "HEAD", "autospec/empty")
        .expect_err("reject empty execution id");

    assert!(matches!(error, WorktreeError::Create(message) if message.contains("execution_id")));
}

#[test]
fn create_rejects_repository_label_mismatch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let mut labels = labels("project-12-impl-01", repository.canonical());
    labels.repository = "different/target".to_owned();

    let error = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-12-impl-01",
        )
        .expect_err("reject mismatched repository ownership");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(!state.path().join("worktrees/project-12-impl-01").exists());
}

#[test]
fn mirror_rejects_noncanonical_repository_identity() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);

    for invalid in [
        "owner",
        "/owner/project",
        "owner/project/extra",
        "owner/../project",
        "owner__/project",
        "owner/project__fork",
    ] {
        let error = manager
            .ensure_mirror(invalid)
            .expect_err("reject noncanonical repository identity");
        assert!(matches!(error, WorktreeError::InvalidRepository(_)));
    }
}

#[test]
fn create_reports_execution_that_owns_checked_out_branch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let first_labels = labels("project-11-impl-01", repository.canonical());
    manager
        .create(
            &first_labels,
            repository.canonical(),
            "HEAD",
            "autospec/shared-branch",
        )
        .expect("create first worktree");

    let second_labels = labels("project-11-review-01", repository.canonical());
    let error = manager
        .create(
            &second_labels,
            repository.canonical(),
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
    let manager = manager(&state, &repository);
    let labels = labels("project-13-impl-01", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
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
    let manager = manager(&state, &repository);
    let labels = labels("project-13-impl-02", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
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
fn capture_diff_refuses_checked_out_branch_switch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-13-impl-03", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-13-impl-03",
        )
        .expect("create worktree");
    git(
        Path::new(&worktree.path),
        &["branch", "-m", "autospec/diff-switched-branch"],
    );

    let error = manager
        .capture_diff(&worktree)
        .expect_err("reject switched checkout branch");

    assert!(matches!(error, WorktreeError::Ownership(_)));
}

#[test]
fn capture_diff_refuses_tampered_owner_repository() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-13-impl-04", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-13-impl-04",
        )
        .expect("create worktree");
    let owner_path = Path::new(&worktree.path).join(".autospec-owner.json");
    let mut owner: Value =
        serde_json::from_slice(&std::fs::read(&owner_path).expect("read owner record"))
            .expect("parse owner record");
    owner["labels"]["autospec.repository"] = Value::String("different/repository".to_owned());
    std::fs::write(
        owner_path,
        serde_json::to_vec_pretty(&owner).expect("serialize owner record"),
    )
    .expect("tamper owner repository");

    let error = manager
        .capture_diff(&worktree)
        .expect_err("reject tampered owner repository");

    assert!(matches!(error, WorktreeError::Ownership(_)));
}

#[test]
fn destroy_removes_only_the_owned_worktree_and_branch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let repository_path = repository.canonical();
    let mirror = manager
        .ensure_mirror(repository_path)
        .expect("create mirror");
    let labels = labels("project-14-impl-01", repository.canonical());
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

#[cfg(unix)]
#[test]
fn destroy_retries_branch_cleanup_from_durable_journal() {
    use std::os::unix::fs::PermissionsExt;

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let mirror = manager
        .ensure_mirror(repository.canonical())
        .expect("create mirror");
    let labels = labels("project-14-impl-04", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-14-impl-04",
        )
        .expect("create worktree");
    let hook = Path::new(&mirror).join("hooks/reference-transaction");
    std::fs::create_dir_all(hook.parent().expect("hook parent")).expect("create hooks directory");
    std::fs::write(&hook, "#!/bin/sh\nexit 1\n").expect("write rejecting Git hook");
    let mut permissions = std::fs::metadata(&hook)
        .expect("read hook metadata")
        .permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(&hook, permissions).expect("make hook executable");

    let first_error = manager
        .destroy(&worktree)
        .expect_err("branch cleanup is rejected");

    assert!(matches!(first_error, WorktreeError::Cleanup(_)));
    assert!(!Path::new(&worktree.path).exists());
    let journal = state
        .path()
        .join("worktrees/.cleanup-project-14-impl-04.json");
    assert!(journal.is_file());
    assert!(git_succeeds(
        state.path(),
        &[
            "--git-dir",
            &mirror,
            "show-ref",
            "--verify",
            "refs/heads/autospec/project-14-impl-04",
        ],
    ));

    std::fs::remove_file(hook).expect("remove rejecting Git hook");
    manager.destroy(&worktree).expect("retry cleanup");

    assert!(!journal.exists());
    assert!(!git_succeeds(
        state.path(),
        &[
            "--git-dir",
            &mirror,
            "show-ref",
            "--verify",
            "refs/heads/autospec/project-14-impl-04",
        ],
    ));
}

#[test]
fn destroy_refuses_tampered_owner_record() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-14-impl-02", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
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
fn destroy_refuses_recorded_and_checked_out_branch_mismatch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-14-impl-05", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-14-impl-05",
        )
        .expect("create worktree");
    git(
        Path::new(&worktree.path),
        &["branch", "-m", "autospec/renamed-outside-owner-record"],
    );

    let error = manager
        .destroy(&worktree)
        .expect_err("refuse branch identity mismatch");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(Path::new(&worktree.path).is_dir());
    assert_eq!(
        git_output(Path::new(&worktree.path), &["branch", "--show-current"]),
        "autospec/renamed-outside-owner-record"
    );
}

#[test]
fn destroy_refuses_tampered_recorded_branch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-14-impl-06", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-14-impl-06",
        )
        .expect("create worktree");
    let owner_path = Path::new(&worktree.path).join(".autospec-owner.json");
    let mut owner: Value =
        serde_json::from_slice(&std::fs::read(&owner_path).expect("read owner record"))
            .expect("parse owner record");
    owner["branch"] = Value::String("autospec/different-recorded-branch".to_owned());
    std::fs::write(
        &owner_path,
        serde_json::to_vec_pretty(&owner).expect("serialize owner record"),
    )
    .expect("tamper recorded branch");

    let error = manager
        .destroy(&worktree)
        .expect_err("refuse tampered recorded branch");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(Path::new(&worktree.path).is_dir());
}

#[test]
fn destroy_refuses_tampered_owner_repository() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-14-impl-07", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-14-impl-07",
        )
        .expect("create worktree");
    let owner_path = Path::new(&worktree.path).join(".autospec-owner.json");
    let mut owner: Value =
        serde_json::from_slice(&std::fs::read(&owner_path).expect("read owner record"))
            .expect("parse owner record");
    owner["labels"]["autospec.repository"] = Value::String("different/repository".to_owned());
    std::fs::write(
        owner_path,
        serde_json::to_vec_pretty(&owner).expect("serialize owner record"),
    )
    .expect("tamper owner repository");

    let error = manager
        .destroy(&worktree)
        .expect_err("refuse tampered owner repository");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(Path::new(&worktree.path).is_dir());
}

#[test]
fn find_stale_returns_owned_worktrees_not_in_live_set() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let repository_path = repository.canonical();
    let live_labels = labels("project-14-impl-03", repository.canonical());
    manager
        .create(
            &live_labels,
            repository_path,
            "HEAD",
            "autospec/project-14-impl-03",
        )
        .expect("create live worktree");
    let stale_labels = labels("project-14-review-01", repository.canonical());
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
    assert_eq!(stale[0].repository, repository.canonical());
}

#[test]
fn find_stale_surfaces_malformed_owner_record() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let path = state.path().join("worktrees/project-14-review-02");
    std::fs::create_dir_all(&path).expect("create malformed worktree directory");
    std::fs::write(path.join(".autospec-owner.json"), "not json\n")
        .expect("write malformed owner record");

    let error = manager
        .find_stale(&[])
        .expect_err("surface malformed owner record");

    assert!(matches!(error, WorktreeError::Cleanup(_)));
}

#[cfg(unix)]
#[test]
fn find_stale_rejects_symlinked_execution_directory() {
    use std::os::unix::fs::symlink;

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let external = tempfile::tempdir().expect("create external directory");
    std::fs::create_dir_all(state.path().join("worktrees")).expect("create worktrees root");
    symlink(
        external.path(),
        state.path().join("worktrees/project-14-review-03"),
    )
    .expect("create symlinked execution directory");

    let error = manager
        .find_stale(&[])
        .expect_err("reject symlinked execution directory");

    assert!(matches!(error, WorktreeError::Ownership(_)));
}
