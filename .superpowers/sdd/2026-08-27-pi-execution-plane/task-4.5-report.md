# Task 4.5 Slice A Report: Execution storage foundation

## Outcome

Added the standalone `execution-storage` crate and workspace membership. The
crate defines the synchronous object-safe storage boundary required before Git,
Pi, Docker, or worker integration changes: deterministic execution-root layout,
checked allocation sizing, durable phase journals, exact allocation receipts,
fail-closed APFS and thick-LVM backends, and an explicit Docker bind-verification
capability contract.

This slice intentionally stops at the foundation. Existing Git, Pi, Docker, and
worker paths are unchanged and must not claim `disk_gib` enforcement until they
consume a verified allocation receipt.

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
  before physical creation. Exact token, pool UUID, and object UUID are then
  fsynced before format; the filesystem UUID is fsynced before mount. Releasing
  journals record mounted, unmounted, and object-absent cleanup subphases.
- `JournalStore` creates the first journal with `create_new`, and later updates
  use a create-new temporary file, file fsync, atomic rename, and directory fsync.
  State paths are canonical, owner-only, and guarded by non-following metadata,
  opened-file inode checks, and pinned directory device/inode/owner/mode checks.
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

### Linux thick LVM

- Requires root and individually probes absolute `lvm`, `mkfs.ext4`, `mount`,
  `umount`, `findmnt`, and `blkid` programs.
- Reads exact VG UUID, free bytes, and extent size. It creates a thick linear LV
  with an ownership-token LVM tag, discovers and journals the VG/LV UUIDs, then
  formats ext4, journals the filesystem UUID, and mounts with `nodev,nosuid`.
- Re-verifies VG/LV UUIDs, exact LV size, device path, ext4 mount target, and
  filesystem UUID before returning or releasing an allocation.
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

## Verification

- `cargo test -p execution-storage -- --nocapture` — 27 passed. The real Docker
  bind verifier contract ran. The destructive aggregate quota lifecycle printed
  an explicit skip because no operator pool is configured on this host; when
  configured it performs create, identity proof, aggregate over-limit writes,
  unmount, and exact release, and configuration failures are test failures.
- `cargo clippy -p execution-storage --all-targets -- -D warnings` — passed.
- Current toolchain `cargo fmt --all -- --check`, `cargo build --workspace`,
  `cargo test --workspace -- --nocapture`, and
  `cargo clippy --workspace --all-targets -- -D warnings` — passed, including
  all real-Docker runtime tests.
- Rust 1.85 ran the same full fmt/build/test/clippy workspace matrix — passed,
  including all real-Docker runtime tests.

## Remaining Integration Work

- Git, Pi, Docker, and worker lifecycle consumers remain on their old layouts by
  design; later Task 4.5 slices must switch them only after receiving a verified
  receipt.
- Destructive real APFS/LVM allocation requires `AUTOSPEC_APFS_PROBE_PATH` or
  `AUTOSPEC_LVM_VOLUME_GROUP` plus appropriate privilege. Neither operator-pool
  configuration is present on this host, so only that test was skipped.
- The Docker verifier is a neutral interface in this crate. A later runtime slice
  must implement daemon identity and bind-filesystem proof without creating a
  dependency cycle.
