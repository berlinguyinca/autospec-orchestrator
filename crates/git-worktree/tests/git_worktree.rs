use execution_storage::{
    AllocationReceipt, BackendIdentity, DockerBindProof, ReadyAllocationVerifier, StorageError,
    VerifiedExecutionStorage, ALLOCATION_API_VERSION,
};
use git_worktree::{
    GitWorktreeManager, SystemWorktreeFilesystem, WorktreeError, WorktreeFilesystem,
    WorktreeFilesystemPoint, WorktreeManager,
};
use orchestrator_core::{ExecutionId, OwnershipLabels, WorkerId};
use serde_json::Value;
use std::io;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

#[derive(Debug)]
struct FakeReadyAllocationVerifier;

#[derive(Debug)]
struct FakeVerifiedExecutionStorage {
    repository: PathBuf,
}

impl VerifiedExecutionStorage for FakeVerifiedExecutionStorage {
    fn repository_path(&self) -> &Path {
        &self.repository
    }

    fn verify(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

impl ReadyAllocationVerifier for FakeReadyAllocationVerifier {
    fn verify_ready(
        &self,
        receipt: &AllocationReceipt,
    ) -> Result<Box<dyn VerifiedExecutionStorage>, StorageError> {
        Ok(Box::new(FakeVerifiedExecutionStorage {
            repository: receipt.mount_path.join("repository"),
        }))
    }
}

struct TestRepository {
    _temp: TempDir,
    path: PathBuf,
    clone_base: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InjectedFailure {
    AlternateAfterClone,
    CloneAfterCopy,
    CloneAndRollbackCreate,
    CloneAndRollbackRemove,
    FaultPoint(WorktreeFilesystemPoint),
    OwnerRecord,
    Remove,
}

#[derive(Debug)]
struct InjectedFilesystem {
    system: SystemWorktreeFilesystem,
    failure: Mutex<Option<InjectedFailure>>,
}

impl InjectedFilesystem {
    fn new(failure: InjectedFailure) -> Self {
        Self {
            system: SystemWorktreeFilesystem,
            failure: Mutex::new(Some(failure)),
        }
    }

    fn take(&self, expected: InjectedFailure) -> bool {
        let mut failure = self.failure.lock().expect("filesystem failure lock");
        if *failure == Some(expected) {
            *failure = None;
            true
        } else {
            false
        }
    }

    fn is(&self, expected: InjectedFailure) -> bool {
        *self.failure.lock().expect("filesystem failure lock") == Some(expected)
    }
}

impl WorktreeFilesystem for InjectedFilesystem {
    fn clone_repository(&self, mirror: &Path, destination: &Path) -> io::Result<()> {
        self.system.clone_repository(mirror, destination)?;
        if self.take(InjectedFailure::AlternateAfterClone) {
            let alternates = destination.join(".git/objects/info/alternates");
            std::fs::create_dir_all(alternates.parent().expect("alternates parent"))?;
            std::fs::write(
                alternates,
                mirror.join("objects").to_string_lossy().as_bytes(),
            )?;
        }
        if self.take(InjectedFailure::CloneAfterCopy)
            || self.is(InjectedFailure::CloneAndRollbackCreate)
            || self.is(InjectedFailure::CloneAndRollbackRemove)
        {
            Err(io::Error::from_raw_os_error(28))
        } else {
            Ok(())
        }
    }

    fn checkpoint(&self, point: WorktreeFilesystemPoint, _path: &Path) -> io::Result<()> {
        if self.take(InjectedFailure::FaultPoint(point))
            || (point == WorktreeFilesystemPoint::OwnerTemporaryWritten
                && self.take(InjectedFailure::OwnerRecord))
        {
            Err(io::Error::from_raw_os_error(28))
        } else {
            Ok(())
        }
    }

    fn remove_repository(&self, path: &Path) -> io::Result<()> {
        if self.take(InjectedFailure::Remove) || self.take(InjectedFailure::CloneAndRollbackRemove)
        {
            Err(io::Error::from_raw_os_error(28))
        } else {
            self.system.remove_repository(path)
        }
    }

    fn create_repository_root(&self, path: &Path) -> io::Result<()> {
        if self.take(InjectedFailure::CloneAndRollbackCreate) {
            Err(io::Error::from_raw_os_error(28))
        } else {
            self.system.create_repository_root(path)
        }
    }
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

#[cfg(unix)]
fn real_git_program() -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
        .map(|directory| directory.join("git"))
        .find(|candidate| candidate.is_file())
        .expect("Git executable on PATH")
        .canonicalize()
        .expect("canonical Git executable")
}

fn bounded_repository_root(state: &TempDir, execution_id: &str) -> PathBuf {
    let path = state
        .path()
        .join("executions")
        .join(execution_id)
        .join("repository");
    std::fs::create_dir_all(&path).expect("create bounded repository root");
    path
}

fn allocation_receipt(state_root: &Path, labels: &OwnershipLabels) -> AllocationReceipt {
    let root = state_root
        .join("executions")
        .join(labels.execution_id.as_str());
    let filesystem_id = format!("filesystem-{}", labels.execution_id);
    AllocationReceipt {
        api_version: ALLOCATION_API_VERSION.to_owned(),
        labels: labels.clone(),
        reserved_bytes: 1024 * 1024 * 1024,
        mount_path: root.clone(),
        backend_kind: "test".to_owned(),
        backend_key: "test-pool".to_owned(),
        pool_identity: "test-pool-id".to_owned(),
        backend: BackendIdentity::Apfs {
            container: "test-container".to_owned(),
            container_uuid: "test-container-uuid".to_owned(),
            volume: "test-volume".to_owned(),
            volume_name: format!("test-{}", labels.execution_id),
            volume_uuid: filesystem_id.clone(),
            ownership_token: format!("token-{}", labels.execution_id),
        },
        docker_bind: DockerBindProof {
            daemon_id: "test-daemon".to_owned(),
            verifier: "test-verifier".to_owned(),
            method_version: "test/v1".to_owned(),
            source_path: root,
            filesystem_id,
        },
    }
}

fn assert_empty_repository_root(state: &TempDir, execution_id: &str) {
    let root = state
        .path()
        .join("executions")
        .join(execution_id)
        .join("repository");
    assert!(root.is_dir());
    assert_eq!(
        std::fs::read_dir(root)
            .expect("read rolled back repository root")
            .count(),
        0
    );
}

struct TestManager {
    inner: GitWorktreeManager,
    state_root: PathBuf,
}

impl TestManager {
    fn create(
        &self,
        labels: &OwnershipLabels,
        repo: &str,
        base_ref: &str,
        branch: &str,
    ) -> Result<git_worktree::Worktree, WorktreeError> {
        let root = self
            .state_root
            .join("executions")
            .join(labels.execution_id.as_str())
            .join("repository");
        if labels
            .execution_id
            .as_str()
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            std::fs::create_dir_all(&root)
                .map_err(|error| WorktreeError::Create(error.to_string()))?;
        }
        let receipt = allocation_receipt(&self.state_root, labels);
        self.inner
            .create_in(labels, repo, base_ref, branch, &receipt)
    }
}

impl Deref for TestManager {
    type Target = GitWorktreeManager;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

fn manager(state: &TempDir, repository: &TestRepository) -> TestManager {
    TestManager {
        inner: GitWorktreeManager::with_clone_base_and_verifier(
            state.path(),
            repository.clone_base.to_str().expect("utf-8 clone base"),
            Arc::new(FakeReadyAllocationVerifier),
        ),
        state_root: state.path().to_path_buf(),
    }
}

fn manager_with_filesystem(
    state: &TempDir,
    repository: &TestRepository,
    filesystem: Arc<dyn WorktreeFilesystem>,
) -> GitWorktreeManager {
    GitWorktreeManager::with_clone_base_filesystem_and_verifier(
        state.path(),
        repository.clone_base.to_str().expect("utf-8 clone base"),
        filesystem,
        Arc::new(FakeReadyAllocationVerifier),
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
        Some(
            state
                .path()
                .join("mirrors")
                .canonicalize()
                .expect("canonical mirror root")
                .as_path()
        )
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

#[cfg(unix)]
#[test]
fn mirror_refuses_symlinked_mirror_root_without_touching_external_directory() {
    use std::os::unix::fs::symlink;

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let external = tempfile::tempdir().expect("create external mirror directory");
    symlink(external.path(), state.path().join("mirrors")).expect("symlink mirror root");
    let manager = manager(&state, &repository);

    let error = manager
        .ensure_mirror(repository.canonical())
        .expect_err("reject symlinked mirror root");

    assert!(matches!(error, WorktreeError::Mirror(_)));
    assert_eq!(
        std::fs::read_dir(external.path())
            .expect("read external mirror directory")
            .count(),
        0
    );
}

#[cfg(unix)]
#[test]
fn mirror_refuses_symlinked_existing_repository_without_fetching() {
    use std::os::unix::fs::symlink;

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let mirror = PathBuf::from(
        manager
            .ensure_mirror(repository.canonical())
            .expect("create mirror"),
    );
    let external = state.path().join("foreign-mirror.git");
    std::fs::rename(&mirror, &external).expect("move mirror outside managed root");
    symlink(&external, &mirror).expect("replace mirror with symlink");
    let refs_before = git_output(
        &external,
        &["for-each-ref", "--format=%(refname):%(objectname)"],
    );
    repository.commit("new upstream commit\n");

    let error = manager
        .ensure_mirror(repository.canonical())
        .expect_err("reject symlinked mirror repository");

    assert!(matches!(error, WorktreeError::Mirror(_)));
    assert_eq!(
        git_output(
            &external,
            &["for-each-ref", "--format=%(refname):%(objectname)"],
        ),
        refs_before
    );
}

#[test]
fn mirror_refuses_non_bare_repository_identity() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let mirror = PathBuf::from(
        manager
            .ensure_mirror(repository.canonical())
            .expect("create mirror"),
    );
    git(&mirror, &["config", "core.bare", "false"]);

    let error = manager
        .ensure_mirror(repository.canonical())
        .expect_err("reject non-bare mirror identity");

    assert!(matches!(error, WorktreeError::Mirror(_)));
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
fn mirror_rejects_separator_boundary_collisions_without_blocking_valid_identity() {
    let mut repository = TestRepository::new();
    let valid = repository.clone_base.join("a/b.git");
    std::fs::create_dir_all(valid.parent().expect("valid repository parent"))
        .expect("create valid repository parent");
    std::fs::rename(&repository.path, &valid).expect("move repository");
    repository.path = valid;
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);

    for colliding in ["a_/b", "a/_b"] {
        let error = manager
            .ensure_mirror(colliding)
            .expect_err("reject separator boundary collision");
        assert!(matches!(error, WorktreeError::InvalidRepository(_)));
    }
    assert!(!state.path().join("mirrors/a___b.git").exists());

    let mirror = manager
        .ensure_mirror("a/b")
        .expect("create valid identity after rejected collisions");
    assert_eq!(
        Path::new(&mirror)
            .file_name()
            .and_then(|name| name.to_str()),
        Some("a__b.git")
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
        state
            .path()
            .join("executions/project-11-impl-01/repository")
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

#[test]
fn create_in_materializes_all_git_state_inside_the_bounded_repository_root() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let execution_id = "project-11-impl-02";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);

    let worktree = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-02",
            &receipt,
        )
        .expect("create independent repository");

    let canonical_root = root.canonicalize().expect("canonical repository root");
    for arguments in [
        &["rev-parse", "--git-dir"][..],
        &["rev-parse", "--git-common-dir"][..],
        &["rev-parse", "--git-path", "objects"][..],
        &["rev-parse", "--git-path", "refs"][..],
        &["rev-parse", "--git-path", "index"][..],
    ] {
        let git_path = PathBuf::from(git_output(&root, arguments));
        let resolved = if git_path.is_absolute() {
            git_path
        } else {
            root.join(git_path)
        }
        .canonicalize()
        .expect("canonical Git path");
        assert!(
            resolved.starts_with(&canonical_root),
            "{arguments:?} escaped bounded root: {}",
            resolved.display()
        );
    }
    assert_eq!(
        git_output(&root, &["remote", "get-url", "origin"]),
        repository.path.to_string_lossy()
    );
    assert_eq!(Path::new(&worktree.path), root);
}

#[test]
fn create_in_rejects_receipt_without_live_ready_verification() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = GitWorktreeManager::with_clone_base(
        state.path(),
        repository.clone_base.to_str().expect("utf-8 clone base"),
    );
    let execution_id = "project-11-unverified-01";
    let labels = labels(execution_id, repository.canonical());
    bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);

    let error = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-unverified-01",
            &receipt,
        )
        .expect_err("reject a structurally valid but unverified receipt");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert_empty_repository_root(&state, execution_id);
    assert!(!state.path().join("mirrors").exists());
}

#[test]
fn create_in_rejects_storage_receipt_filesystem_identity_mismatch() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let execution_id = "project-11-storage-01";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let mut receipt = allocation_receipt(state.path(), &labels);
    receipt.docker_bind.filesystem_id = "different-filesystem".to_owned();

    let error = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-storage-01",
            &receipt,
        )
        .expect_err("reject mismatched storage filesystem identity");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert_eq!(
        std::fs::read_dir(root)
            .expect("read untouched repository")
            .count(),
        0
    );
}

#[test]
fn execution_commit_does_not_mutate_mirror_objects_or_refs() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let mirror = PathBuf::from(
        manager
            .ensure_mirror(repository.canonical())
            .expect("create mirror"),
    );
    let objects_before = git_output(&mirror, &["count-objects", "-v"]);
    let refs_before = git_output(
        &mirror,
        &["for-each-ref", "--format=%(refname):%(objectname)"],
    );
    let execution_id = "project-11-impl-03";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);
    manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-03",
            &receipt,
        )
        .expect("create independent repository");

    std::fs::write(root.join("large.bin"), vec![0x5a; 2 * 1024 * 1024])
        .expect("write large execution object");
    git(&root, &["add", "large.bin"]);
    git(&root, &["commit", "-m", "execution-only commit"]);

    assert_eq!(
        git_output(&mirror, &["count-objects", "-v"]),
        objects_before
    );
    assert_eq!(
        git_output(
            &mirror,
            &["for-each-ref", "--format=%(refname):%(objectname)"],
        ),
        refs_before
    );
}

#[test]
fn clone_rejects_alternate_object_database_and_rolls_back() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let filesystem = Arc::new(InjectedFilesystem::new(
        InjectedFailure::AlternateAfterClone,
    ));
    let manager = manager_with_filesystem(&state, &repository, filesystem);
    let execution_id = "project-11-impl-07";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);

    let error = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-07",
            &receipt,
        )
        .expect_err("reject alternate object database");

    assert!(matches!(
        error,
        WorktreeError::Ownership(_) | WorktreeError::Create(_)
    ));
    assert_empty_repository_root(&state, execution_id);
    assert!(!root.join(".git/objects/info/alternates").exists());
}

#[test]
fn git_commands_ignore_hostile_repository_routing_environment() {
    const CHILD: &str = "AUTOSPEC_GIT_HOSTILE_ENV_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "git_commands_ignore_hostile_repository_routing_environment",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .expect("run isolated hostile-environment test");
        assert!(
            output.status.success(),
            "hostile environment child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let hostile = tempfile::tempdir().expect("create hostile Git directory");
    let hostile_index = hostile.path().join("index");
    for (key, value) in [
        ("GIT_DIR", hostile.path().as_os_str()),
        ("GIT_OBJECT_DIRECTORY", hostile.path().as_os_str()),
        ("GIT_INDEX_FILE", hostile_index.as_os_str()),
        ("GIT_WORK_TREE", hostile.path().as_os_str()),
        ("GIT_NAMESPACE", std::ffi::OsStr::new("hostile")),
        (
            "GIT_CONFIG_GLOBAL",
            hostile.path().join("config").as_os_str(),
        ),
    ] {
        std::env::set_var(key, value);
    }
    std::env::set_var("GIT_ALTERNATE_OBJECT_DIRECTORIES", hostile.path());
    let labels = labels("project-11-impl-08", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-08",
        )
        .expect("create despite hostile Git routing environment");
    std::fs::write(Path::new(&worktree.path).join("evidence.txt"), "bounded\n")
        .expect("write evidence");
    manager
        .capture_diff(&worktree)
        .expect("capture despite hostile Git routing environment");
    assert!(!hostile_index.exists());
}

#[test]
fn clone_enospc_rolls_back_partial_repository_without_mutating_mirror() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let filesystem = Arc::new(InjectedFilesystem::new(InjectedFailure::CloneAfterCopy));
    let manager = manager_with_filesystem(&state, &repository, filesystem);
    let mirror = PathBuf::from(
        manager
            .ensure_mirror(repository.canonical())
            .expect("create mirror"),
    );
    let objects_before = git_output(&mirror, &["count-objects", "-v"]);
    let refs_before = git_output(
        &mirror,
        &["for-each-ref", "--format=%(refname):%(objectname)"],
    );
    let execution_id = "project-11-impl-04";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);

    let error = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-04",
            &receipt,
        )
        .expect_err("surface clone ENOSPC");

    assert!(matches!(error, WorktreeError::StorageFull(_)));
    assert!(root.is_dir());
    assert_eq!(
        std::fs::read_dir(&root)
            .expect("read rolled back repository")
            .count(),
        0
    );
    assert_eq!(
        git_output(&mirror, &["count-objects", "-v"]),
        objects_before
    );
    assert_eq!(
        git_output(
            &mirror,
            &["for-each-ref", "--format=%(refname):%(objectname)"],
        ),
        refs_before
    );
}

#[cfg(unix)]
#[test]
fn real_git_clone_enospc_is_distinct_and_restart_recovers_partial_state() {
    use std::os::unix::fs::PermissionsExt;

    const CHILD: &str = "AUTOSPEC_REAL_GIT_ENOSPC_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "real_git_clone_enospc_is_distinct_and_restart_recovers_partial_state",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .expect("run isolated ENOSPC test");
        assert!(
            output.status.success(),
            "ENOSPC child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    manager
        .ensure_mirror(repository.canonical())
        .expect("seed mirror before hostile PATH");
    let execution_id = "project-11-real-enospc-01";
    let labels = labels(execution_id, repository.canonical());
    bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);
    let bin = tempfile::tempdir().expect("fake Git bin");
    let wrapper = bin.path().join("git");
    let real_git = real_git_program();
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nif [ \"$1\" = clone ]; then\n  for arg do destination=$arg; done\n  mkdir -p \"$destination/.git/objects/pack\"\n  echo 'fatal: write error: No space left on device' >&2\n  exit 28\nfi\nexec '{}' \"$@\"\n",
            real_git.display()
        ),
    )
    .expect("write Git wrapper");
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700))
        .expect("make Git wrapper executable");
    let original_path = std::env::var_os("PATH").expect("PATH");
    let path = std::env::join_paths(
        std::iter::once(bin.path().to_path_buf()).chain(std::env::split_paths(&original_path)),
    )
    .expect("wrapper PATH");
    std::env::set_var("PATH", &path);

    let error = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-real-enospc-01",
            &receipt,
        )
        .expect_err("surface real Git ENOSPC");
    assert!(
        matches!(error, WorktreeError::StorageFull(_)),
        "unexpected error: {error:?}"
    );
    std::env::set_var("PATH", original_path);

    let worktree = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-real-enospc-01",
            &receipt,
        )
        .expect("restart after exact rollback");
    assert!(Path::new(&worktree.path).join(".git/objects").is_dir());
}

#[cfg(unix)]
#[test]
fn real_git_checkout_enospc_is_distinct_and_restart_recovers_partial_index() {
    use std::os::unix::fs::PermissionsExt;

    const CHILD: &str = "AUTOSPEC_REAL_GIT_CHECKOUT_ENOSPC_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "real_git_checkout_enospc_is_distinct_and_restart_recovers_partial_index",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .expect("run isolated checkout ENOSPC test");
        assert!(
            output.status.success(),
            "checkout ENOSPC child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    manager
        .ensure_mirror(repository.canonical())
        .expect("seed mirror before hostile PATH");
    let execution_id = "project-11-checkout-enospc";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);
    let bin = tempfile::tempdir().expect("fake Git bin");
    let wrapper = bin.path().join("git");
    let real_git = real_git_program();
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\ncheckout=0\nfor arg do [ \"$arg\" = checkout ] && checkout=1; done\nif [ \"$checkout\" = 1 ]; then\n  printf partial > '{}/.git/index'\n  echo 'fatal: index write failed: No space left on device' >&2\n  exit 28\nfi\nexec '{}' \"$@\"\n",
            root.display(),
            real_git.display()
        ),
    )
    .expect("write checkout Git wrapper");
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700))
        .expect("make Git wrapper executable");
    let original_path = std::env::var_os("PATH").expect("PATH");
    let path = std::env::join_paths(
        std::iter::once(bin.path().to_path_buf()).chain(std::env::split_paths(&original_path)),
    )
    .expect("wrapper PATH");
    std::env::set_var("PATH", &path);

    let error = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-checkout-enospc",
            &receipt,
        )
        .expect_err("surface real checkout ENOSPC");
    assert!(matches!(error, WorktreeError::StorageFull(_)));
    std::env::set_var("PATH", original_path);

    let worktree = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-checkout-enospc",
            &receipt,
        )
        .expect("restart after partial index rollback");
    assert_eq!(
        git_output(Path::new(&worktree.path), &["branch", "--show-current"]),
        "autospec/project-11-checkout-enospc"
    );
}

#[test]
fn owner_record_enospc_rolls_back_cloned_repository() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let filesystem = Arc::new(InjectedFilesystem::new(InjectedFailure::OwnerRecord));
    let manager = manager_with_filesystem(&state, &repository, filesystem);
    let execution_id = "project-11-impl-05";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);

    let error = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-05",
            &receipt,
        )
        .expect_err("surface owner-record ENOSPC");

    assert!(matches!(error, WorktreeError::StorageFull(_)));
    assert!(root.is_dir());
    assert_eq!(
        std::fs::read_dir(&root)
            .expect("read rolled back repository")
            .count(),
        0
    );
}

#[test]
fn create_intent_recovers_partial_clone_after_restart() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let filesystem = Arc::new(InjectedFilesystem::new(
        InjectedFailure::CloneAndRollbackRemove,
    ));
    let manager = manager_with_filesystem(&state, &repository, filesystem);
    let execution_id = "project-11-impl-09";
    let labels = labels(execution_id, repository.canonical());
    let journaled_base = git_output(&repository.path, &["rev-parse", "HEAD"]);
    bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);

    manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-09",
            &receipt,
        )
        .expect_err("leave durable intent after rollback failure");
    let intent = state
        .path()
        .join("worktrees/.create-project-11-impl-09.json");
    let value: Value = serde_json::from_slice(&std::fs::read(&intent).expect("read create intent"))
        .expect("parse create intent");
    assert_eq!(value["phase"], "cloning");
    assert_eq!(value["base_sha"], journaled_base);
    let temporary_intent = intent.with_file_name(format!(
        "{}.tmp",
        intent
            .file_name()
            .and_then(|name| name.to_str())
            .expect("intent filename")
    ));
    std::fs::write(&temporary_intent, b"{\"partial\":")
        .expect("simulate interrupted replacement intent write");
    #[cfg(unix)]
    std::fs::set_permissions(&temporary_intent, std::fs::Permissions::from_mode(0o600))
        .expect("secure temporary intent mode");
    repository.commit("upstream advanced after interrupted clone\n");

    let restarted = GitWorktreeManager::with_clone_base_and_verifier(
        state.path(),
        repository.clone_base.to_str().expect("utf-8 clone base"),
        Arc::new(FakeReadyAllocationVerifier),
    );
    let worktree = restarted
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-09",
            &receipt,
        )
        .expect("recover exact intent and recreate repository");

    assert!(!intent.exists());
    assert!(!temporary_intent.exists());
    assert_eq!(worktree.base_sha, journaled_base);
    assert!(Path::new(&worktree.path)
        .join(".autospec-owner.json")
        .is_file());
}

#[test]
fn create_intent_recovery_requires_live_verification_before_mutation() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let filesystem = Arc::new(InjectedFilesystem::new(
        InjectedFailure::CloneAndRollbackRemove,
    ));
    let manager = manager_with_filesystem(&state, &repository, filesystem);
    let execution_id = "project-11-live-before-recovery";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);

    manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-live-before-recovery",
            &receipt,
        )
        .expect_err("leave durable partial clone and intent");
    let intent = state
        .path()
        .join("worktrees/.create-project-11-live-before-recovery.json");
    let intent_before = std::fs::read(&intent).expect("read durable intent");
    assert!(root.join(".git").is_dir());

    let unverified = GitWorktreeManager::with_clone_base_and_filesystem(
        state.path(),
        repository.clone_base.to_str().expect("utf-8 clone base"),
        Arc::new(SystemWorktreeFilesystem),
    );
    let error = unverified
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-live-before-recovery",
            &receipt,
        )
        .expect_err("reject recovery without live storage capability");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(root.join(".git").is_dir());
    assert_eq!(
        std::fs::read(intent).expect("intent retained"),
        intent_before
    );
}

#[test]
fn create_intent_recovers_when_prior_rollback_left_repository_absent() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let filesystem = Arc::new(InjectedFilesystem::new(
        InjectedFailure::CloneAndRollbackCreate,
    ));
    let manager = manager_with_filesystem(&state, &repository, filesystem);
    let execution_id = "project-11-impl-rollback-absent";
    let labels = labels(execution_id, repository.canonical());
    let root = bounded_repository_root(&state, execution_id);
    let receipt = allocation_receipt(state.path(), &labels);

    manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-rollback-absent",
            &receipt,
        )
        .expect_err("leave durable intent after recreate failure");
    assert!(!root.exists());

    let restarted = GitWorktreeManager::with_clone_base_and_verifier(
        state.path(),
        repository.clone_base.to_str().expect("utf-8 clone base"),
        Arc::new(FakeReadyAllocationVerifier),
    );
    let worktree = restarted
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-11-impl-rollback-absent",
            &receipt,
        )
        .expect("recover absent exact repository root");

    assert_eq!(Path::new(&worktree.path), root);
    assert!(root.join(".autospec-owner.json").is_file());
}

#[test]
fn enospc_at_git_and_owner_commit_phases_rolls_back_exact_repository() {
    for (index, point) in [
        WorktreeFilesystemPoint::CloneObjectPack,
        WorktreeFilesystemPoint::CheckoutIndex,
        WorktreeFilesystemPoint::OwnerTemporaryWritten,
        WorktreeFilesystemPoint::OwnerTemporarySynced,
        WorktreeFilesystemPoint::OwnerRenamed,
    ]
    .into_iter()
    .enumerate()
    {
        let repository = TestRepository::new();
        let state = tempfile::tempdir().expect("create state root");
        let filesystem = Arc::new(InjectedFilesystem::new(InjectedFailure::FaultPoint(point)));
        let manager = manager_with_filesystem(&state, &repository, filesystem);
        let execution_id = format!("project-11-enospc-{index}");
        let labels = labels(&execution_id, repository.canonical());
        bounded_repository_root(&state, &execution_id);
        let receipt = allocation_receipt(state.path(), &labels);

        let error = manager
            .create_in(
                &labels,
                repository.canonical(),
                "HEAD",
                &format!("autospec/{execution_id}"),
                &receipt,
            )
            .expect_err("surface injected ENOSPC");

        assert!(matches!(
            error,
            WorktreeError::StorageFull(_) | WorktreeError::Ownership(_)
        ));
        assert_empty_repository_root(&state, &execution_id);
        assert!(!state
            .path()
            .join(format!("worktrees/.create-{execution_id}.json"))
            .exists());
    }
}

#[test]
fn legacy_unbounded_create_fails_closed() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-11-impl-06", repository.canonical());

    let error = WorktreeManager::create(
        &*manager,
        &labels,
        repository.canonical(),
        "HEAD",
        "autospec/project-11-impl-06",
    )
    .expect_err("legacy create cannot escape execution storage");

    assert!(matches!(error, WorktreeError::Create(message) if message.contains("disabled")));
    assert!(!state.path().join("worktrees/project-11-impl-06").exists());
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
    assert_empty_repository_root(&state, "project-12-impl-02");
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
    assert_empty_repository_root(&state, "project-12-impl-03");
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
    assert_empty_repository_root(&state, "project-12-impl-01");
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

#[cfg(unix)]
#[test]
fn branch_collision_scan_rejects_symlinked_repository_entry() {
    use std::os::unix::fs::symlink;

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let first_labels = labels("project-11-review-02", repository.canonical());
    let first = manager
        .create(
            &first_labels,
            repository.canonical(),
            "HEAD",
            "autospec/symlink-scan",
        )
        .expect("create first repository");
    std::fs::remove_dir_all(&first.path).expect("remove first repository");
    let foreign = tempfile::tempdir().expect("create foreign directory");
    symlink(foreign.path(), &first.path).expect("replace repository with symlink");
    let second_labels = labels("project-11-review-03", repository.canonical());

    let error = manager
        .create(
            &second_labels,
            repository.canonical(),
            "HEAD",
            "autospec/symlink-scan",
        )
        .expect_err("reject symlinked repository during branch scan");

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(foreign.path().is_dir());
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

#[cfg(unix)]
#[test]
fn capture_diff_never_executes_repository_configured_helpers() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-13-hostile-diff", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-13-hostile-diff",
        )
        .expect("create worktree");
    let worktree_path = Path::new(&worktree.path);
    let marker = state.path().join("hostile-helper-ran");
    let helper = state.path().join("hostile-helper");
    std::fs::write(
        &helper,
        format!("#!/bin/sh\ntouch '{}'\nexit 0\n", marker.display()),
    )
    .expect("write hostile helper");
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700))
        .expect("make hostile helper executable");
    let hooks = state.path().join("hostile-hooks");
    std::fs::create_dir(&hooks).expect("create hostile hooks directory");
    std::fs::write(
        hooks.join("post-checkout"),
        std::fs::read(&helper).expect("read helper"),
    )
    .expect("write hostile hook");
    std::fs::set_permissions(
        hooks.join("post-checkout"),
        std::fs::Permissions::from_mode(0o700),
    )
    .expect("make hostile hook executable");
    let included = state.path().join("hostile-include.config");
    std::fs::write(
        &included,
        format!("[diff]\n\texternal = {}\n", helper.display()),
    )
    .expect("write hostile included config");
    git(
        worktree_path,
        &[
            "config",
            "diff.hostile.textconv",
            helper.to_str().expect("helper path"),
        ],
    );
    git(
        worktree_path,
        &[
            "config",
            "diff.external",
            helper.to_str().expect("helper path"),
        ],
    );
    git(
        worktree_path,
        &[
            "config",
            "core.fsmonitor",
            helper.to_str().expect("helper path"),
        ],
    );
    git(
        worktree_path,
        &[
            "config",
            "core.hooksPath",
            hooks.to_str().expect("hooks path"),
        ],
    );
    git(
        worktree_path,
        &[
            "config",
            "include.path",
            included.to_str().expect("include path"),
        ],
    );
    std::fs::write(
        worktree_path.join(".gitattributes"),
        "README.md diff=hostile\n",
    )
    .expect("write hostile attributes");
    std::fs::write(
        worktree_path.join("README.md"),
        "modified without helpers\n",
    )
    .expect("modify tracked file");

    let capture = manager.capture_diff(&worktree).expect("capture safe diff");

    assert!(capture.patch.contains("modified without helpers"));
    assert!(!marker.exists(), "repository-controlled helper executed");
}

#[cfg(unix)]
#[test]
fn capture_diff_rejects_commondir_before_invoking_git() {
    const CHILD: &str = "AUTOSPEC_COMMONDIR_PREFLIGHT_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().expect("current test executable"))
            .args([
                "--exact",
                "capture_diff_rejects_commondir_before_invoking_git",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .expect("run isolated commondir test");
        assert!(
            output.status.success(),
            "commondir child failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }

    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let manager = manager(&state, &repository);
    let labels = labels("project-13-commondir", repository.canonical());
    let worktree = manager
        .create(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-13-commondir",
        )
        .expect("create worktree");
    std::fs::write(
        Path::new(&worktree.path).join(".git/commondir"),
        "../foreign\n",
    )
    .expect("write forbidden commondir");
    let marker = state.path().join("git-wrapper-ran");
    let bin = tempfile::tempdir().expect("wrapper bin");
    let wrapper = bin.path().join("git");
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\ntouch '{}'\nexit 99\n", marker.display()),
    )
    .expect("write Git wrapper");
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700))
        .expect("make Git wrapper executable");
    let original_path = std::env::var_os("PATH").expect("PATH");
    let path = std::env::join_paths(
        std::iter::once(bin.path().to_path_buf()).chain(std::env::split_paths(&original_path)),
    )
    .expect("wrapper PATH");
    std::env::set_var("PATH", &path);

    let error = manager
        .capture_diff(&worktree)
        .expect_err("reject forbidden commondir");
    std::env::set_var("PATH", original_path);

    assert!(matches!(error, WorktreeError::Ownership(_)));
    assert!(!marker.exists(), "Git ran before filesystem preflight");
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
    let foreign = state.path().join("executions/foreign/repository");
    std::fs::create_dir_all(&foreign).expect("create foreign repository");
    std::fs::write(foreign.join("keep"), "foreign\n").expect("write foreign sentinel");

    manager.destroy(&worktree).expect("destroy worktree");

    assert!(!Path::new(&worktree.path).exists());
    assert_eq!(
        std::fs::read_to_string(foreign.join("keep")).expect("read foreign sentinel"),
        "foreign\n"
    );
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
fn destroy_retries_repository_cleanup_from_durable_journal() {
    let repository = TestRepository::new();
    let state = tempfile::tempdir().expect("create state root");
    let filesystem = Arc::new(InjectedFilesystem::new(InjectedFailure::Remove));
    let manager = manager_with_filesystem(&state, &repository, filesystem);
    let labels = labels("project-14-impl-04", repository.canonical());
    bounded_repository_root(&state, labels.execution_id.as_str());
    let receipt = allocation_receipt(state.path(), &labels);
    let worktree = manager
        .create_in(
            &labels,
            repository.canonical(),
            "HEAD",
            "autospec/project-14-impl-04",
            &receipt,
        )
        .expect("create worktree");

    let first_error = manager
        .destroy(&worktree)
        .expect_err("repository cleanup hits ENOSPC");

    assert!(matches!(first_error, WorktreeError::Cleanup(_)));
    assert!(Path::new(&worktree.path).exists());
    let journal = state
        .path()
        .join("worktrees/.cleanup-project-14-impl-04.json");
    assert!(journal.is_file());

    manager.destroy(&worktree).expect("retry cleanup");

    assert!(!journal.exists());
    assert!(!Path::new(&worktree.path).exists());
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
    let path = state
        .path()
        .join("executions/project-14-review-02/repository");
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
    std::fs::create_dir_all(state.path().join("executions")).expect("create executions root");
    symlink(
        external.path(),
        state.path().join("executions/project-14-review-03"),
    )
    .expect("create symlinked execution directory");

    let error = manager
        .find_stale(&[])
        .expect_err("reject symlinked execution directory");

    assert!(matches!(error, WorktreeError::Ownership(_)));
}
