# Task 4.5 Report: Execution storage foundation and Git migration

## Outcome

Added the standalone `execution-storage` crate and workspace membership. The
crate defines the synchronous object-safe storage boundary required before Git,
Pi, Docker, or worker integration changes: deterministic execution-root layout,
checked allocation sizing, durable phase journals, exact allocation receipts,
fail-closed APFS and thick-LVM backends, and an explicit Docker bind-verification
capability contract.

The foundation slice initially stopped before consumer migration. Git, Docker,
and Pi now consume deterministic bounded paths from a verified allocation
receipt. Worker lifecycle orchestration must still carry these exact receipts
and leases end to end before claiming complete `disk_gib` enforcement.

## Pi Migration

- Added `PiHarnessConfig::for_ready_allocation`, which accepts one exact
  `AllocationReceipt` plus `Arc<dyn ReadyAllocationVerifier>` and derives the
  repository, private session, and conversation paths solely from
  `ExecutionLayout`. The legacy arbitrary `state_root`/`worktree` constructor is
  retained for compatibility but fails closed before creating files or starting
  a Docker process.
- Revalidates the receipt, exact ownership labels, deterministic layout, live
  Ready journal/backend/bind identity, repository path, and pinned repository,
  session, conversation, and `.autospec` directories before mutation. Static
  directory symlinks and file-level symlinks for event/control records are
  rejected without following them outside the allocation.
- Acquires a shared `ReadyLease` before task-packet/session mutation and moves it
  into the registered Pi process. Stop and drop retain the lease through TERM,
  KILL, and confirmed reap; resume and fork acquire their own live-session lease,
  while cursor polling uses a temporary lease when it mutates durable reducer
  state. Releasing therefore cannot race a running or mutating Pi session.
- Moved the private session root from the legacy
  `state_root/sessions/{execution_id}` path to exact `layout.session`. Owner,
  cursor/reducer, resume count, live JSONL, and stderr stay there; Pi receives
  only `layout.conversation` at `/session`. The compact packet is materialized
  once at `layout.repository/.autospec/task-packet.json` and remains referenced
  as `@/workspace/.autospec/task-packet.json`.
- Hardened direct-child harness records with real-file identity checks,
  owner-only permissions, collision-resistant create-new temporary files, and
  atomic rename. Conversation discovery ignores symlink entries, and torn-tail
  repair uses the same bounded atomic writer.
- Implementation commit: `4a6487f`.

## Git Migration

- Added `WorktreeManager::create_in` for the exact
  `executions/{execution_id}/repository` directory supplied from verified
  execution storage. The legacy unbounded `create` entry point fails closed.
- Replaced linked `git worktree add` repositories with copy-producing
  `git clone --no-local --no-hardlinks --no-checkout` materialization. The
  execution repository resets `origin` to the configured canonical clone
  locator, so no execution remote references the worker mirror.
- Verified `.git`, the common directory, object database, refs, index, and lock
  location all resolve beneath the bounded repository root.
- Kept mirrors as locked, exact-origin-verified input/update infrastructure.
  A large execution-only commit leaves mirror object and ref inventories
  unchanged.
- Preserved repository/branch ownership validation, injective mirror naming,
  diff evidence, branch collision locks, stale discovery, owner-record safety,
  and durable retryable cleanup journals on the new execution layout.
- Added an injectable `WorktreeFilesystem` boundary. Real-Git regressions inject
  ENOSPC after a partial clone and during owner-record commit, prove exact
  rollback to an empty bounded repository directory, and prove cleanup retries
  from durable exact ownership without touching foreign resources.
- Centralized every production Git subprocess behind an environment-clearing
  command builder. Hostile repository, object, index, worktree, namespace,
  alternate-object, and config routing variables cannot redirect mirror,
  creation, evidence, or cleanup commands.
- `create_in` now consumes an `execution-storage` allocation receipt rather than
  a bare path. It validates exact labels/layout/backend/bind identities, derives
  the deterministic repository path, pins the execution root and repository
  directory, and requires same-filesystem direct ancestry throughout creation.
- Pinned the canonical private mirror root and revalidated exact non-symlinked
  ownership, bare identity, canonical parent, and origin under lock immediately
  before every update and execution clone.
- Added a durable fsynced create-intent journal outside the bounded filesystem
  with exact labels, repository, base, branch, path, and creation phase. Restart
  recovery rolls back only the exact matching partial repository; mismatched or
  malformed intents fail closed.
- Expanded repository storage verification after clone and checkout and during
  evidence and cleanup: Git directory, common directory, objects, refs, index,
  and derived locks must stay beneath the execution repository, with no
  alternate-object file or symlink escape.
- Added phase-specific ENOSPC seams for cloned object packs, checkout index, and
  owner-record temporary write, fsync, and rename; successful rollback removes
  the create intent, while rollback failure retains it for exact restart
  recovery.
- Replaced receipt-only path trust with `ReadyAllocationVerifier` and a pinned
  `VerifiedExecutionStorage` capability. The real execution-storage verifier
  exact-matches the durable Ready journal, labels, backend configuration,
  ownership token, mounted backend state, filesystem identity, and Docker bind
  proof immediately before clone materialization; legacy constructors reject
  structurally valid receipts when no live verifier is configured.
- Routed Git create and cleanup journals through execution-storage's reusable
  owner-only `SecureMetadataDirectory`. It uses no-follow opens, opened inode
  checks, file and directory fsync, and reconciles durable write/removal
  temporaries after restart.
- Made create rollback idempotent when an earlier attempt removed the exact
  repository directory, recreated it empty, or failed between those phases.
  Recovery remains limited to the deterministic direct child selected by the
  exact journal labels and path.
- Moved interrupted-create recovery before mirror refresh and retained the
  journaled `base_sha`; an upstream ref advance during downtime cannot silently
  retarget the restarted execution.
- Added filesystem-only Git storage and alternate-object preflight immediately
  before repository Git invocations used by owner scans, stale discovery,
  evidence, and destruction.
- Added PATH-wrapper regressions that make real Git leave partial object-pack
  and index state and exit with ENOSPC during clone and checkout. ENOSPC is now
  a distinct `WorktreeError::StorageFull`, and restart removes only the exact
  partial repository before retrying.
- Hardened host-side evidence capture against repository-controlled execution.
  Capture now snapshots the verified index into a unique owner-only Git
  directory beneath pinned host metadata, uses a detached trusted HEAD, and
  explicitly selects that Git directory plus the execution work tree. The
  mutable execution `.git/config` is never read. Git reads objects through an
  exact verified `GIT_OBJECT_DIRECTORY`; the trusted context contains no agent
  config for SHA-1 repositories and only the allowlisted SHA-256 object-format
  scalars when required. Every child still starts from a cleared environment
  with system/global config and attributes disabled, and diff commands disable
  external diffs and text conversion.
- Reject `.git/commondir` during the filesystem-only preflight before every
  repository Git command. A PATH wrapper regression proves malformed linked
  common-directory metadata is rejected without starting Git.
- Require the live pinned execution-storage capability before reading or
  mutating interrupted-create state, including when the repository child is
  absent. The capability pins the execution mountpoint, pins the repository
  when present, and permits an explicit exact-recovery repin only after the
  deterministic journal-selected rollback.
- Retain the original exact-base create intent while recovery runs. Its durable
  replacement is committed atomically before clone resumes; a partial
  replacement temporary is discarded while the original base remains the
  recovery authority, so upstream movement cannot retarget the execution.
- Changed both create-intent metadata and execution-storage journal recovery to
  discard uncommitted orphan temporary files after no-follow/opened-inode
  validation. Partial JSON is never promoted into authoritative state.
- Evidence capture no longer performs the prior O(N*U) config-and-attribute
  rescans around each Git invocation. The trusted Git context is established
  once after the execution-stop boundary. Capture never copies the mutable
  execution index or reads the execution object database: it refreshes and
  exact-verifies the owned bare mirror, holds that mirror's repository lock for
  the full capture, pins its alternate-free object database, and runs
  `read-tree` for the exact journaled base SHA into the host-private index. The
  index and metadata directory are fsynced and removed through the pinned
  metadata boundary after capture. Safe boolean, unset, and unspecified `diff`
  attributes remain supported because repository filter and diff commands are
  unavailable in the trusted context.
- Added hostile `filter.clean`, long-running `filter.process`, repository helper,
  and synchronized config/attribute mutation regressions. Even when a Git PATH
  wrapper changes the execution config and attributes immediately before the
  first diff, no marker program starts and tracked/untracked evidence remains
  complete.
- Assume-unchanged and skip-worktree bits, a malformed execution index, and
  synchronized replacement of execution objects, alternates, and pack data no
  longer affect evidence. Dirty submodules are ignored explicitly so capture
  never enters a mutable child Git repository or starts its configured filter.
- Every capture Git command receives the exact host-private trusted `--git-dir`
  and execution `--work-tree` selectors. Repository verification also requires
  `rev-parse --show-toplevel` to canonicalize to that exact root.
- Metadata-subdirectory creation is transactional after the direct child is
  created. Injected parent-fsync and child-pinning failures remove only the
  exact inode just created before returning the original failure.
- Metadata-subdirectory rollback now begins with the first post-creation child
  inspection. Injected child-metadata failure removes only the exact new child
  while an unrelated sibling and its contents remain unchanged.
- Trusted evidence contexts own both Git-produced `index` and `index.lock`
  artifacts before `read-tree` starts. Failure cleanup opens each optional file
  with no-follow semantics, verifies the opened inode, restricts it through the
  descriptor, and exact-removes it before removing the capture directory. A
  fake Git ENOSPC regression leaves permissive partial files and proves restart
  cleanup removes them without touching a foreign metadata sibling.
- The storage manager's full `verify_ready(receipt)` transition check runs a
  second time immediately before the first clone write. A stateful verifier
  regression proves a receipt that leaves Ready after preparation cannot write
  into the execution allocation.

## Public Contract

- `ExecutionLayout` maps a validated execution ID to
  `executions/{execution_id}/` with `repository/`, `session/conversation/`,
  `credentials/`, and `runtime/` children. Journals live at the exact metadata
  path `execution-storage/{execution_id}.json` outside the mounted filesystem.
- `disk_gib_to_bytes` performs exact checked GiB conversion and rejects zero or
  overflow.
- `AllocationReceipt` uses
  `autospec.dev/execution-storage/v1alpha1` and retains the exact five ownership
  labels, requested byte reservation, mount path, backend identity, filesystem
  identity, Docker daemon identity, verifier identity, proof-method version, and
  bind source proof.
- `PhaseJournal` records `Allocating`, `Ready`, and `Releasing`. The allocating
  journal is created exclusively and fsynced with a unique ownership token
  before physical creation. It also records the probed backend kind, configured
  backend key, and pool UUID; recovery re-probes and exact-matches all three
  before discovery or cleanup. Exact token, pool UUID, and object UUID are then
  fsynced before format; the filesystem UUID is fsynced before mount. Releasing
  journals record mounted, unmounted, and object-absent cleanup subphases.
- `JournalStore` creates the first journal with `create_new`, and later updates
  use a create-new temporary file, file fsync, atomic rename, and directory fsync.
  State paths are canonical, owner-only, and guarded by non-following metadata,
  opened-file inode checks, and pinned directory device/inode/owner/mode checks.
  All child mutations route through the internal `PinnedDirectory` boundary.
  Linux resolves relative children through `/proc/self/fd` with `O_NOFOLLOW`.
  The direct `libc` dependency supplies only the portable `O_NOFOLLOW` constant;
  it introduces no FFI calls or unsafe code.
  macOS cannot traverse `/dev/fd/{dirfd}/child`, so true descriptor-relative
  `openat`/`renameat`/`unlinkat` remains blocked by the workspace unsafe-code ban
  and the prohibition on a new safe syscall-wrapper dependency.
- `ExecutionStorageManager` is synchronous, `Send + Sync`, and object-safe. Its
  probe combines backend reservation capacity with an explicit Docker
  daemon/verifier capability. Allocation fails closed unless both prove usable;
  release requires an exact receipt match; reconciliation reports orphan
  journals and deletes nothing.

## Backends

### macOS APFS

- Requires root for reserved APFS volume creation and mounting.
- Probes the exact writable APFS container and unallocated capacity.
- Creates the token-named volume with equal quota/reserve and `-nomount`, reads
  the exact container UUID and volume UUID, and durably records them before the
  separate mount operation.
- Re-reads token-derived name, device, container UUID, volume UUID, mountpoint,
  read-only state, quota, and reserve before every unmount or deletion.
- Treats absence only as a successful exact container inventory with no matching
  token name. `diskutil` command errors remain unknown and propagate. Volume
  headers and created devices are parsed exactly, including prefix-collision
  regressions.

### Linux thick LVM

- Requires root and individually probes absolute `lvm`, `mkfs.ext4`, `mount`,
  `umount`, `findmnt`, and `blkid` programs.
- Reads exact VG UUID, free bytes, and extent size. It creates a thick linear LV
  with an ownership-token LVM tag, discovers and journals the VG/LV UUIDs, then
  formats ext4, journals the filesystem UUID, and mounts with `nodev,nosuid`.
- Re-verifies VG/LV UUIDs, exact LV size, device path, ext4 mount target, and
  filesystem UUID before returning or releasing an allocation.
- Treats absence/unmounted state only as successful structured `lvs`/`findmnt`
  inventory with no exact match; command errors never become absence.
- VG input accepts the documented safe character grammar while rejecting a
  leading hyphen, dot names, and LVM-reserved names/prefixes; supported commands
  place `--` before operator-controlled names.

All commands use an absolute program plus an argument vector through injectable
`CommandRunner`; there are no shell strings, sparse images, glob deletions,
polling loops, or `du`/`df` accounting.

## TDD Evidence

- Initial crate test failed on all missing layout, receipt, and journal symbols.
- Manager tests then failed on missing object-safe allocation/release/reconcile
  contracts. The first rollback test left its journal behind after Docker proof
  failure; cleanup now removes backend, mountpoint, and journal only after exact
  rollback succeeds.
- APFS rollback test initially left the exact created volume command unconsumed
  after quota/reserve verification failed. The backend now deletes that reported
  device and no other volume.
- Thick-LVM rollback test initially left exact `umount` and `lvremove` commands
  unconsumed after a mismatched `findmnt` proof. Both cleanup classes are now
  attempted and errors are aggregated.
- The allocation journal identity test initially failed because Allocating could
  not retain backend identity. The journal is now fsynced again immediately after
  backend creation, closing that crash-recovery identity gap.
- Docker readiness tests initially lacked a capability probe. Manager probe now
  requires non-empty daemon and verifier identities and rejects a daemon change
  between probe and allocation proof.
- A release regression test showed that APFS/LVM may remove the mountpoint while
  releasing the exact backend object. Release now accepts an already-absent
  mountpoint after backend identity verification while continuing to reject a
  surviving symlink, non-directory, or non-empty directory.
- A rollback regression replaced the mountpoint with a dangling symlink during
  Docker proof. Non-following metadata inspection now retains the journal,
  reports cleanup failure, and leaves the foreign path untouched.
- Review-round reds proved the prior combined backend allocation could format or
  mount before durable object identity. The backend contract is now split into
  discover/create/prepare/mount/state/unmount/remove, with a journal fsync at
  each identity boundary.
- Recovery-order coverage initially showed capability probing occurred before a
  stale object was released. Existing journals now route recovery first so the
  stranded object cannot make its own pool appear too full to recover.
- Release recovery tests resume successfully from mounted, exact-unmounted, and
  exact-already-absent states without overwriting the prior journal.
- Security tests reject permissive state-root modes, symlink paths, and replaced
  directory inodes. Docker contract tests reject proof-method drift and exercise
  a concrete ownership-labelled real bind proof when Docker is available.
- Review-round coverage proves pool/config drift is rejected before discovery,
  APFS preparation is identity-preserving, and LVM preparation may change only
  an empty filesystem UUID to one non-empty UUID.
- The concrete Docker verifier no longer accepts a caller-supplied identity or
  marker. It starts an ownership-labelled, networkless, read-only container and
  constructs the proof from daemon-side `stat` device/inode identity.
- Pi integration reds first failed because no receipt-backed constructor existed.
  A forged-receipt verifier then proved rejection occurs before packet, owner,
  cursor, or process creation. The lease test blocked a simulated Releasing
  transition until `stop` reaped the in-container group. A file-level regression
  initially followed a pre-planted live-event symlink outside the allocation;
  append/read/atomic record paths now reject that escape before Pi starts.

## Verification

- `cargo test -p git-worktree` — 58 real temporary Git repository tests passed,
  including hostile Git environments, independent Git-path containment, mirror
  immutability/substitution rejection, receipt binding, create-intent recovery,
  phase-specific ENOSPC rollback, safe diff capture, pre-Git commondir rejection,
  exact cleanup, foreign-resource survival, and stale discovery.
- `cargo test -p execution-storage -- --nocapture` — 41 passed. The real Docker
  bind verifier contract ran. The destructive aggregate quota lifecycle printed
  an explicit skip because no operator pool is configured on this host; when
  configured it performs create, identity proof, substantial successful writes,
  aggregate over-limit writes accepted only as ENOSPC/StorageFull,
  unmount, and exact release, and configuration failures are test failures.
- `cargo test -p harness-pi --all-targets -- --nocapture` — 26 passed against
  production-shaped labelled Docker containers without `--init`. New coverage
  proves stale/forged receipt rejection before writes, fail-closed legacy paths,
  Ready→Releasing exclusion through stop, exact allocation-root placement,
  pinned directory verification, conversation-only `/session` exposure, and
  directory/file symlink rejection without escaping private storage.
- `cargo clippy -p execution-storage -p git-worktree --all-targets -- -D warnings`
  — passed.
- Current toolchain `cargo fmt --all -- --check`, `cargo build --workspace`,
  `cargo test --workspace`, and
  `cargo clippy --workspace --all-targets -- -D warnings` — passed, including
  all real-Docker runtime tests.
- Rust 1.85 ran the same full fmt/build/test/clippy workspace matrix — passed,
  including all 58 Git tests, 26 Pi real-Docker tests, and 18 runtime-Docker
  tests.
- The first current-toolchain workspace test attempt saw the existing
  five-second `harness-pi` drop-bound test exceed its timing threshold under
  concurrent Docker load. The isolated retry passed in 4.76 seconds, and the
  subsequent full current and Rust 1.85 workspace runs both passed. The current
  integration now passes all 26 Pi harness tests on both toolchains.

## Pi/runtime capability fix round 1

- Added the neutral `VerifiedAgentContainer` runtime capability. Docker runtime
  issues it only after inspecting the immutable created container ID, live
  running state, exact ownership-label map, daemon identity, and exact
  daemon-reported bind set. The proof includes canonical host sources,
  container targets, and writability.
- `PiHarnessConfig::for_ready_allocation` now requires that capability rather
  than accepting an arbitrary container name. Before packet, owner, cursor, or
  process mutation, the harness re-inspects Docker by immutable ID and requires
  the same daemon, running container, labels, and mounts. It rejects any bind
  outside the allocation root, any private session path other than
  `conversation/`, and the Docker socket. It repeats the live proof immediately
  before opening event/stderr output and launching Pi.
- Added a real-Docker foreign-container regression. A running, identically
  mounted but unlabeled replacement ID is rejected specifically by the live
  ownership check; packet, owner, cursor, resume counter, and Pi body markers
  remain absent.
- Cleanup liveness now treats only the supervisor's explicit exit 0 as a
  runnable group and exit 1 as zombie-only/absent. Wrapper, daemon, timeout, and
  other control failures are uncertain rather than accidental proof of reap.
- Uncertain startup or `ProcessRegistry::drop` cleanup transfers the Docker
  client, trusted PGID/token authority, and `ReadyLease` to a fallible,
  nonblocking quarantine reaper. A host-private journal is written before the
  handoff; retries retain the lease until both the client and zombie-aware
  process-group checks confirm death. If the retry thread cannot start or
  receive, the authority is deliberately retained rather than releasing
  storage.
- Added startup and drop fault-injection tests whose Docker control commands
  fail after Pi launch. Both prove release remains blocked while cleanup is
  uncertain, then succeeds only after control recovers and no runnable Pi or
  descendant remains.
- Cursor initialization now writes the normalized absolute per-session live
  event path only for an empty default. Every nonempty stored path must be byte-
  for-byte equal to that exact path; legacy, relative, external, or alternate
  normalized spellings fail before the referenced file is opened.
- Implementation commit: `84e05c1`.

### Fix-round verification

- `cargo test -p harness-pi --test pi_harness -- --test-threads=1` — 30/30
  production-shaped real-Docker tests passed without `--init`.
- `cargo test -p runtime-docker --test docker_runtime
  agent_mounts_only_writable_worktree_and_durable_conversation -- --nocapture`
  — passed with immutable ID, daemon, label, and exact mount assertions.
- Current toolchain: `cargo fmt --all -- --check`, `cargo build --workspace`,
  `cargo test --workspace`, and
  `cargo clippy --workspace --all-targets -- -D warnings` — passed, including
  30 Pi, 18 runtime-Docker, 58 real-Git, and 4 Postgres tests.
- Rust 1.85.0: full workspace build/test and warning-denied Clippy — passed with
  the same Docker, Git, and Postgres integration suites.
- `git diff --check` — passed.

## Remaining Integration Work

- Round-2 filesystem finding 3 is not fully closed on macOS. The exact missing
  safe surface is descriptor-relative `openat` with `O_NOFOLLOW|O_CREAT|O_EXCL`,
  `renameat`, `unlinkat`, `mkdirat`, `fstatat(AT_SYMLINK_NOFOLLOW)`, and directory
  `fsync`. Raw libc requires forbidden unsafe blocks; `/dev/fd` child traversal
  returns ENOENT on this host. A direct safe `rustix` filesystem dependency
  (already transitive in `Cargo.lock`) or a narrowly audited unsafe exception is
  required. No unsafe or `rustix` dependency was added in this round.

- Worker lifecycle orchestration must still pass the same receipt/verifier
  capability through Git, runtime, and Pi construction and release only after
  every retained consumer lease is dropped.
- Destructive real APFS/LVM allocation requires `AUTOSPEC_APFS_PROBE_PATH` or
  `AUTOSPEC_LVM_VOLUME_GROUP` plus appropriate privilege. Neither operator-pool
  configuration is present on this host, so only that test was skipped.
- The Docker verifier is a neutral interface in this crate. A later runtime slice
  must implement daemon identity and bind-filesystem proof without creating a
  dependency cycle.
