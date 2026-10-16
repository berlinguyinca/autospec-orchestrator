use crate::DockerRuntime;
use bollard::{
    container::{ListContainersOptions, RemoveContainerOptions},
    network::ListNetworksOptions,
    volume::{ListVolumesOptions, RemoveVolumeOptions},
};
use orchestrator_core::{labels, ExecutionId, OwnershipLabels};
use runtime_traits::build_cache::{is_managed_build_cache_dir, REFS_DIR, REF_GRACE};
use runtime_traits::RuntimeError;
use std::{
    collections::{BTreeSet, HashMap},
    fs,
    path::{Path, PathBuf},
    time::{Duration, SystemTime},
};

pub(crate) async fn destroy(
    runtime: &DockerRuntime,
    labels: &OwnershipLabels,
) -> Result<(), RuntimeError> {
    runtime.require_compatible_daemon().await?;
    runtime.require_cleanup_authority(labels).await?;
    let filters = label_filters(labels.selector());
    let mut errors = Vec::new();

    match runtime
        .client
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters: filters.clone(),
            ..Default::default()
        }))
        .await
    {
        Ok(containers) => {
            for container in containers {
                if let Some(id) = container.id {
                    if let Err(error) = runtime
                        .client
                        .remove_container(
                            &id,
                            Some(RemoveContainerOptions {
                                force: true,
                                v: true,
                                link: false,
                            }),
                        )
                        .await
                    {
                        errors.push(format!("remove owned container {id}: {error}"));
                    }
                }
            }
        }
        Err(error) => errors.push(format!("list owned containers: {error}")),
    }

    match runtime
        .client
        .list_volumes(Some(ListVolumesOptions {
            filters: filters.clone(),
        }))
        .await
    {
        Ok(volumes) => {
            for volume in volumes.volumes.unwrap_or_default() {
                let name = volume.name;
                if let Err(error) = runtime
                    .client
                    .remove_volume(&name, Some(RemoveVolumeOptions { force: true }))
                    .await
                {
                    errors.push(format!("remove owned volume {name}: {error}"));
                }
            }
        }
        Err(error) => errors.push(format!("list owned volumes: {error}")),
    }

    match runtime
        .client
        .list_networks(Some(ListNetworksOptions { filters }))
        .await
    {
        Ok(networks) => {
            for network in networks {
                if let Some(id) = network.id {
                    if let Err(error) = runtime.client.remove_network(&id).await {
                        errors.push(format!("remove owned network {id}: {error}"));
                    }
                }
            }
        }
        Err(error) => errors.push(format!("list owned networks: {error}")),
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(RuntimeError::Cleanup(format!(
            "execution_id={}: {}",
            labels.execution_id,
            errors.join("; ")
        )))
    }
}

pub(crate) async fn reconcile(
    runtime: &DockerRuntime,
    live: &[ExecutionId],
) -> Result<Vec<ExecutionId>, RuntimeError> {
    runtime.require_compatible_daemon().await?;
    let filters = label_filters(vec![format!("{}=true", labels::MANAGED)]);
    let live = live.iter().collect::<BTreeSet<_>>();
    let mut orphans = BTreeSet::new();

    let containers = runtime
        .client
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters: filters.clone(),
            ..Default::default()
        }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list managed containers: {error}")))?;
    for resource_labels in containers.into_iter().filter_map(|item| item.labels) {
        collect_orphan(&resource_labels, &live, &mut orphans);
    }

    let networks = runtime
        .client
        .list_networks(Some(ListNetworksOptions {
            filters: filters.clone(),
        }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list managed networks: {error}")))?;
    for resource_labels in networks.into_iter().filter_map(|item| item.labels) {
        collect_orphan(&resource_labels, &live, &mut orphans);
    }

    let volumes = runtime
        .client
        .list_volumes(Some(ListVolumesOptions { filters }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list managed volumes: {error}")))?;
    for resource_labels in volumes
        .volumes
        .unwrap_or_default()
        .into_iter()
        .map(|item| item.labels)
    {
        collect_orphan(&resource_labels, &live, &mut orphans);
    }

    Ok(orphans.into_iter().collect())
}

/// One shared-cache key directory that is ours (valid owner file) and its
/// measured state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    pub key: String,
    pub bytes: u64,
    pub last_used: SystemTime,
    pub refs: Vec<ExecutionId>,
}

/// What a pressure-GC pass did. `foreign` lists everything under the cache
/// root that is not a managed key directory (no valid owner file, a symlink, a
/// stray file): counted, reported, never touched.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GcReport {
    pub evicted: Vec<String>,
    pub freed_bytes: u64,
    pub kept_live: Vec<String>,
    pub foreign: Vec<String>,
    pub pruned_refs: usize,
    pub total_bytes_after: u64,
}

/// Picks key directories to evict, oldest `last_used` first, until the total
/// size (live entries included, they still occupy disk) is within `budget`.
/// An entry referenced by any execution in `live` is never selected, however
/// old.
pub fn select_evictions(
    entries: &[CacheEntry],
    budget: u64,
    live: &BTreeSet<ExecutionId>,
) -> Vec<String> {
    let mut total: u64 = entries.iter().map(|entry| entry.bytes).sum();
    let mut by_age = entries.iter().collect::<Vec<_>>();
    by_age.sort_by(|a, b| a.last_used.cmp(&b.last_used).then(a.key.cmp(&b.key)));
    let mut selected = Vec::new();
    for entry in by_age {
        if total <= budget {
            break;
        }
        if entry.refs.iter().any(|id| live.contains(id)) {
            continue;
        }
        total = total.saturating_sub(entry.bytes);
        selected.push(entry.key.clone());
    }
    selected
}

/// Execution ids that still own a container on this daemon, found by the
/// ownership label only (the same selector `reconcile` uses).
async fn live_execution_ids(
    runtime: &DockerRuntime,
) -> Result<BTreeSet<ExecutionId>, RuntimeError> {
    runtime.require_compatible_daemon().await?;
    let containers = runtime
        .client
        .list_containers(Some(ListContainersOptions {
            all: true,
            filters: label_filters(vec![format!("{}=true", labels::MANAGED)]),
            ..Default::default()
        }))
        .await
        .map_err(|error| RuntimeError::Cleanup(format!("list managed containers: {error}")))?;
    Ok(containers
        .into_iter()
        .filter_map(|item| item.labels)
        .filter_map(|labels| labels.get(labels::EXECUTION_ID).cloned())
        .map(ExecutionId::new)
        .collect())
}

/// Reclaims shared build-cache space under disk pressure (spec §5 R-RES,
/// R-BUILD; AGENTS.md invariants 6 and 7). Only directories that carry a valid
/// owner file and are not in use are removed; anything else under the cache
/// root is reported in [`GcReport::foreign`] and left alone. Liveness comes
/// from the Docker daemon, and a daemon that cannot be asked aborts the pass
/// without deleting anything.
pub async fn gc_build_cache(runtime: &DockerRuntime) -> Result<GcReport, RuntimeError> {
    let Some(config) = runtime.build_cache.clone() else {
        return Ok(GcReport::default());
    };
    let live = live_execution_ids(runtime).await?;
    let budget = config.max_gib.saturating_mul(1024 * 1024 * 1024);
    tokio::task::spawn_blocking(move || {
        collect_build_cache(&config.root, budget, &live, SystemTime::now(), REF_GRACE)
    })
    .await
    .map_err(|error| RuntimeError::Cleanup(format!("build cache gc task: {error}")))?
}

/// The filesystem half of [`gc_build_cache`], parameterised for tests.
pub(crate) fn collect_build_cache(
    root: &Path,
    budget: u64,
    live: &BTreeSet<ExecutionId>,
    now: SystemTime,
    grace: Duration,
) -> Result<GcReport, RuntimeError> {
    let mut report = GcReport::default();
    let sccache = root.join("sccache");
    if !is_real_dir(root) || !is_real_dir(&sccache) {
        return Ok(report);
    }
    let read = fs::read_dir(&sccache).map_err(|error| {
        RuntimeError::Cleanup(format!("read build cache {}: {error}", sccache.display()))
    })?;
    let mut entries = Vec::new();
    let mut protected = BTreeSet::new();
    for item in read {
        let item = item
            .map_err(|error| RuntimeError::Cleanup(format!("read build cache entry: {error}")))?;
        let name = item.file_name().to_string_lossy().into_owned();
        let path = item.path();
        if !is_real_dir(&path) || !is_managed_build_cache_dir(&path) {
            report.foreign.push(name);
            continue;
        }
        let (bytes, last_used) = measure(&path);
        let refs = read_refs(&path);
        // A ref younger than the grace window belongs to a provisioning that
        // has not created its container yet: it protects the directory.
        if refs.iter().any(|(_, modified)| {
            now.duration_since(*modified)
                .map(|age| age < grace)
                .unwrap_or(true)
        }) {
            protected.insert(name.clone());
        }
        entries.push(CacheEntry {
            key: name,
            bytes,
            last_used,
            refs: refs.into_iter().map(|(id, _)| id).collect(),
        });
    }
    // Protected entries are modelled as referenced by a live execution so the
    // one selection rule covers both cases.
    let placeholder = ExecutionId::new("autospec-gc-grace");
    let mut effective_live = live.clone();
    effective_live.insert(placeholder.clone());
    for entry in &mut entries {
        if protected.contains(&entry.key) {
            entry.refs.push(placeholder.clone());
        }
    }
    let total: u64 = entries.iter().map(|entry| entry.bytes).sum();
    report.total_bytes_after = total;
    for key in select_evictions(&entries, budget, &effective_live) {
        let dir = sccache.join(&key);
        // Re-validate immediately before deleting: still our real directory,
        // still marked, still not claimed since the scan.
        if !is_real_dir(&dir) || !is_managed_build_cache_dir(&dir) {
            report.foreign.push(key);
            continue;
        }
        if read_refs(&dir).iter().any(|(id, modified)| {
            live.contains(id)
                || now
                    .duration_since(*modified)
                    .map(|age| age < grace)
                    .unwrap_or(true)
        }) {
            report.kept_live.push(key);
            continue;
        }
        let bytes = entries
            .iter()
            .find(|entry| entry.key == key)
            .map_or(0, |entry| entry.bytes);
        // `remove_dir_all` unlinks symlinks it meets instead of following them.
        match fs::remove_dir_all(&dir) {
            Ok(()) => {
                report.freed_bytes += bytes;
                report.total_bytes_after = report.total_bytes_after.saturating_sub(bytes);
                report.evicted.push(key);
            }
            Err(error) => {
                return Err(RuntimeError::Cleanup(format!(
                    "evict build cache {}: {error}",
                    dir.display()
                )));
            }
        }
    }
    for entry in &entries {
        if report.evicted.contains(&entry.key) {
            continue;
        }
        if entry.refs.iter().any(|id| live.contains(id)) || protected.contains(&entry.key) {
            report.kept_live.push(entry.key.clone());
        }
        report.pruned_refs += prune_dead_refs(&sccache.join(&entry.key), live, now, grace);
    }
    report.kept_live.sort();
    report.kept_live.dedup();
    Ok(report)
}

fn is_real_dir(path: &Path) -> bool {
    fs::symlink_metadata(path)
        .map(|meta| meta.is_dir() && !meta.file_type().is_symlink())
        .unwrap_or(false)
}

/// Total size and newest modification time of everything under `dir`, never
/// following symlinks (a link counts as its own few bytes, its target is not
/// visited).
fn measure(dir: &Path) -> (u64, SystemTime) {
    let mut bytes = 0u64;
    let mut newest = SystemTime::UNIX_EPOCH;
    let mut stack: Vec<PathBuf> = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(read) = fs::read_dir(&current) else {
            continue;
        };
        for item in read.flatten() {
            let Ok(meta) = fs::symlink_metadata(item.path()) else {
                continue;
            };
            if let Ok(modified) = meta.modified() {
                newest = newest.max(modified);
            }
            if meta.is_dir() && !meta.file_type().is_symlink() {
                stack.push(item.path());
            } else {
                bytes = bytes.saturating_add(meta.len());
            }
        }
    }
    if let Ok(modified) = fs::symlink_metadata(dir).and_then(|meta| meta.modified()) {
        newest = newest.max(modified);
    }
    (bytes, newest)
}

/// `(execution id, ref modified time)` for each regular ref file; symlinks and
/// unsafe names are ignored.
fn read_refs(dir: &Path) -> Vec<(ExecutionId, SystemTime)> {
    let refs = dir.join(REFS_DIR);
    if !is_real_dir(&refs) {
        return Vec::new();
    }
    let Ok(read) = fs::read_dir(&refs) else {
        return Vec::new();
    };
    read.flatten()
        .filter_map(|item| {
            let name = item.file_name().to_str()?.to_owned();
            let meta = fs::symlink_metadata(item.path()).ok()?;
            if !meta.is_file() || meta.file_type().is_symlink() || name.is_empty() {
                return None;
            }
            Some((ExecutionId::new(name), meta.modified().ok()?))
        })
        .collect()
}

/// Removes ref files of executions that are neither live nor within the grace
/// window. Only regular files directly inside this managed directory's `refs`.
fn prune_dead_refs(
    dir: &Path,
    live: &BTreeSet<ExecutionId>,
    now: SystemTime,
    grace: Duration,
) -> usize {
    let mut removed = 0;
    for (id, modified) in read_refs(dir) {
        let fresh = now
            .duration_since(modified)
            .map(|age| age < grace)
            .unwrap_or(true);
        if !live.contains(&id)
            && !fresh
            && fs::remove_file(dir.join(REFS_DIR).join(id.as_str())).is_ok()
        {
            removed += 1;
        }
    }
    removed
}

fn label_filters(selector: Vec<String>) -> HashMap<String, Vec<String>> {
    HashMap::from([("label".to_owned(), selector)])
}

fn collect_orphan(
    resource_labels: &HashMap<String, String>,
    live: &BTreeSet<&ExecutionId>,
    orphans: &mut BTreeSet<ExecutionId>,
) {
    let complete_execution_labels = resource_labels.get(labels::MANAGED).map(String::as_str)
        == Some("true")
        && resource_labels.contains_key(labels::WORKER_ID)
        && resource_labels.contains_key(labels::REPOSITORY);
    if complete_execution_labels {
        let Some(value) = resource_labels.get(labels::EXECUTION_ID) else {
            return;
        };
        let execution_id = ExecutionId::new(value.clone());
        if !live.contains(&execution_id) {
            orphans.insert(execution_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deployment_labels_are_not_execution_orphans() {
        let deployment = HashMap::from([
            (labels::MANAGED.to_owned(), "true".to_owned()),
            (
                labels::EXECUTION_ID.to_owned(),
                "deployment-test".to_owned(),
            ),
        ]);
        let mut orphans = BTreeSet::new();
        collect_orphan(&deployment, &BTreeSet::new(), &mut orphans);
        assert!(orphans.is_empty());
    }

    // ---- shared build cache GC -------------------------------------------

    fn entry(key: &str, bytes: u64, age_secs: u64, refs: &[&str]) -> CacheEntry {
        CacheEntry {
            key: key.to_owned(),
            bytes,
            last_used: SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000 - age_secs),
            refs: refs.iter().map(|id| ExecutionId::new(*id)).collect(),
        }
    }

    fn live(ids: &[&str]) -> BTreeSet<ExecutionId> {
        ids.iter().map(|id| ExecutionId::new(*id)).collect()
    }

    #[test]
    fn select_evictions_keeps_a_live_entry_even_when_it_is_the_oldest() {
        let entries = [
            entry("oldest-live", 50, 900, &["running"]),
            entry("middle", 50, 500, &[]),
            entry("newest", 50, 100, &[]),
        ];
        let picked = select_evictions(&entries, 60, &live(&["running"]));
        assert!(!picked.contains(&"oldest-live".to_owned()));
        assert_eq!(picked, vec!["middle".to_owned(), "newest".to_owned()]);
    }

    #[test]
    fn select_evictions_goes_oldest_first_and_stops_once_under_budget() {
        let entries = [
            entry("c", 40, 100, &[]),
            entry("a", 40, 900, &[]),
            entry("b", 40, 500, &[]),
        ];
        // 120 bytes, budget 70: evicting the oldest (a) leaves 80, then b leaves 40.
        assert_eq!(
            select_evictions(&entries, 70, &live(&[])),
            vec!["a".to_owned(), "b".to_owned()]
        );
        // Budget 80: only the oldest has to go.
        assert_eq!(
            select_evictions(&entries, 80, &live(&[])),
            vec!["a".to_owned()]
        );
        // Already within budget: nothing.
        assert!(select_evictions(&entries, 120, &live(&[])).is_empty());
    }

    #[test]
    fn a_dead_reference_does_not_protect_an_entry() {
        let entries = [entry("a", 100, 900, &["gone"])];
        assert_eq!(
            select_evictions(&entries, 0, &live(&["other"])),
            vec!["a".to_owned()]
        );
    }

    const GRACE: Duration = Duration::from_secs(900);

    fn set_mtime(path: &Path, when: SystemTime) {
        fs::File::open(path).unwrap().set_modified(when).unwrap();
    }

    /// Creates `<root>/sccache/<key>` as a managed cache of `bytes` bytes whose
    /// content is `age` old, with a ref file per `(execution, ref age)`.
    fn make_entry(
        root: &Path,
        key: &str,
        bytes: usize,
        age: Duration,
        refs: &[(&str, Duration)],
        now: SystemTime,
    ) -> PathBuf {
        let dir = root.join("sccache").join(key);
        fs::create_dir_all(dir.join(REFS_DIR)).unwrap();
        fs::write(
            dir.join(runtime_traits::build_cache::OWNER_FILE),
            format!(r#"{{"managed":true,"key":"{key}"}}"#),
        )
        .unwrap();
        fs::create_dir_all(dir.join("objects")).unwrap();
        fs::write(dir.join("objects/blob"), vec![0u8; bytes]).unwrap();
        for (id, ref_age) in refs {
            let marker = dir.join(REFS_DIR).join(id);
            fs::write(&marker, "x").unwrap();
            set_mtime(&marker, now - *ref_age);
        }
        for path in [
            dir.join("objects/blob"),
            dir.join("objects"),
            dir.join(REFS_DIR),
            dir.join(runtime_traits::build_cache::OWNER_FILE),
            dir.clone(),
        ] {
            set_mtime(&path, now - age);
        }
        dir
    }

    fn old() -> Duration {
        Duration::from_secs(48 * 3600)
    }

    #[test]
    fn scan_skips_foreign_entries_and_reports_them_without_touching_them() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("cache");
        let now = SystemTime::now();
        make_entry(&root, "owned-old", 1000, old(), &[], now);
        let sccache = root.join("sccache");
        // No owner file.
        let unmarked = sccache.join("unmarked");
        fs::create_dir_all(&unmarked).unwrap();
        fs::write(unmarked.join("precious"), b"keep").unwrap();
        // Owner file says managed:false.
        let falsy = sccache.join("falsy");
        fs::create_dir_all(&falsy).unwrap();
        fs::write(
            falsy.join(runtime_traits::build_cache::OWNER_FILE),
            br#"{"managed":false}"#,
        )
        .unwrap();
        fs::write(falsy.join("precious"), b"keep").unwrap();
        // A stray regular file.
        fs::write(sccache.join("stray-file"), b"keep").unwrap();

        let report = collect_build_cache(&root, 0, &live(&[]), now, GRACE).unwrap();
        assert_eq!(report.evicted, vec!["owned-old".to_owned()]);
        let mut foreign = report.foreign.clone();
        foreign.sort();
        assert_eq!(foreign, vec!["falsy", "stray-file", "unmarked"]);
        assert!(!sccache.join("owned-old").exists());
        assert_eq!(fs::read(unmarked.join("precious")).unwrap(), b"keep");
        assert_eq!(fs::read(falsy.join("precious")).unwrap(), b"keep");
        assert_eq!(fs::read(sccache.join("stray-file")).unwrap(), b"keep");
    }

    #[cfg(unix)]
    #[test]
    fn a_symlinked_key_dir_is_foreign_and_its_target_survives() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("cache");
        let now = SystemTime::now();
        // A real, managed directory outside the cache, reached through a link.
        let outside = tmp.path().join("outside-managed");
        fs::create_dir_all(&outside).unwrap();
        fs::write(
            outside.join(runtime_traits::build_cache::OWNER_FILE),
            br#"{"managed":true}"#,
        )
        .unwrap();
        fs::write(outside.join("data"), b"keep").unwrap();
        fs::create_dir_all(root.join("sccache")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("sccache/linked")).unwrap();

        let report = collect_build_cache(&root, 0, &live(&[]), now, GRACE).unwrap();
        assert_eq!(report.foreign, vec!["linked".to_owned()]);
        assert!(report.evicted.is_empty());
        assert_eq!(fs::read(outside.join("data")).unwrap(), b"keep");
        assert!(root.join("sccache/linked").exists());
    }

    #[cfg(unix)]
    #[test]
    fn evicting_a_directory_unlinks_inner_symlinks_without_following_them() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("cache");
        let now = SystemTime::now();
        let victim_target = tmp.path().join("victim-target");
        fs::create_dir_all(&victim_target).unwrap();
        fs::write(victim_target.join("precious"), b"keep").unwrap();
        let dir = make_entry(&root, "owned-old", 100, old(), &[], now);
        std::os::unix::fs::symlink(&victim_target, dir.join("objects/escape")).unwrap();
        set_mtime(&dir, now - old());

        let report = collect_build_cache(&root, 0, &live(&[]), now, GRACE).unwrap();
        assert_eq!(report.evicted, vec!["owned-old".to_owned()]);
        assert!(!dir.exists());
        assert_eq!(fs::read(victim_target.join("precious")).unwrap(), b"keep");
    }

    #[test]
    fn live_and_freshly_claimed_entries_are_never_evicted() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("cache");
        let now = SystemTime::now();
        make_entry(
            &root,
            "running",
            500,
            old(),
            &[("exec-running", old())],
            now,
        );
        make_entry(
            &root,
            "just-claimed",
            500,
            old(),
            &[("exec-new", Duration::from_secs(5))],
            now,
        );
        make_entry(&root, "dead-ref", 500, old(), &[("exec-dead", old())], now);

        let report = collect_build_cache(&root, 0, &live(&["exec-running"]), now, GRACE).unwrap();
        assert_eq!(report.evicted, vec!["dead-ref".to_owned()]);
        let mut kept = report.kept_live.clone();
        kept.sort();
        assert_eq!(kept, vec!["just-claimed", "running"]);
        assert!(root.join("sccache/running/objects/blob").is_file());
        assert!(root.join("sccache/just-claimed/objects/blob").is_file());
        assert!(!root.join("sccache/dead-ref").exists());
    }

    #[test]
    fn eviction_is_lru_stops_at_the_budget_and_prunes_dead_refs_of_kept_entries() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path().join("cache");
        let now = SystemTime::now();
        make_entry(
            &root,
            "oldest",
            1000,
            Duration::from_secs(72 * 3600),
            &[],
            now,
        );
        make_entry(
            &root,
            "middle",
            1000,
            Duration::from_secs(48 * 3600),
            &[],
            now,
        );
        make_entry(
            &root,
            "newest",
            1000,
            Duration::from_secs(24 * 3600),
            &[("exec-dead", old()), ("exec-running", old())],
            now,
        );
        let total: u64 = ["oldest", "middle", "newest"]
            .iter()
            .map(|key| measure(&root.join("sccache").join(key)).0)
            .sum();
        // Room for exactly two of the three: one eviction, the oldest.
        let budget = total - total / 3 + 1;
        let report =
            collect_build_cache(&root, budget, &live(&["exec-running"]), now, GRACE).unwrap();
        assert_eq!(report.evicted, vec!["oldest".to_owned()]);
        assert!(root.join("sccache/middle").exists());
        assert!(root.join("sccache/newest").exists());
        assert_eq!(report.pruned_refs, 1);
        assert!(!root.join("sccache/newest/refs/exec-dead").exists());
        assert!(root.join("sccache/newest/refs/exec-running").exists());
    }

    #[test]
    fn a_missing_or_symlinked_cache_root_collects_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let now = SystemTime::now();
        let report =
            collect_build_cache(&tmp.path().join("absent"), 0, &live(&[]), now, GRACE).unwrap();
        assert_eq!(report, GcReport::default());
        #[cfg(unix)]
        {
            let real = tmp.path().join("real");
            make_entry(&real, "owned-old", 10, old(), &[], now);
            std::os::unix::fs::symlink(&real, tmp.path().join("link")).unwrap();
            let report =
                collect_build_cache(&tmp.path().join("link"), 0, &live(&[]), now, GRACE).unwrap();
            assert_eq!(report, GcReport::default());
            assert!(real.join("sccache/owned-old").exists());
        }
    }
}
