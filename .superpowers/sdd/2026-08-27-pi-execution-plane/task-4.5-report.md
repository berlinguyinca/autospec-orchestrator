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
  identity, Docker daemon identity, and bind source proof.
- `PhaseJournal` records `Allocating`, `Ready`, and `Releasing`. The allocating
  phase is fsynced before backend creation and updated with the exact backend
  identity before Docker proof. Ready/releasing journals carry the full receipt.
- `JournalStore` writes a create-new temporary file, flushes and fsyncs it,
  atomically renames it, then fsyncs the journal directory. Reads, replacement,
  removal, and reconciliation reject symlinks and unexpected entry names/types.
- `ExecutionStorageManager` is synchronous, `Send + Sync`, and object-safe. Its
  probe combines backend reservation capacity with an explicit Docker
  daemon/verifier capability. Allocation fails closed unless both prove usable;
  release requires an exact receipt match; reconciliation reports orphan
  journals and deletes nothing.

## Backends

### macOS APFS

- Requires root because `diskutil apfs addVolume ... -mountpoint` documents the
  mountpoint operation as root-only.
- Probes the exact writable APFS container and unallocated capacity.
- Creates a separately-argued `diskutil apfs addVolume` command with both
  `-quota {bytes}b` and `-reserve {bytes}b` set to the same checked byte count.
- Re-reads exact device, container, UUID, mountpoint, read-only state, quota, and
  reserve before returning a receipt or deleting a volume.
- If post-create reservation proof fails, rollback deletes only the exact device
  identifier reported by `addVolume` and aggregates rollback failure.

### Linux thick LVM

- Requires root and individually probes absolute `lvm`, `mkfs.ext4`, `mount`,
  `umount`, `findmnt`, and `blkid` programs.
- Reads exact VG UUID, free bytes, and extent size. It uses a thick linear LV,
  rejects non-exact extent sizing, formats ext4, and mounts with `nodev,nosuid`.
- Re-verifies VG/LV UUIDs, exact LV size, device path, ext4 mount target, and
  filesystem UUID before returning or releasing an allocation.
- Any post-create failure attempts exact unmount when applicable and exact
  `lvremove`, retaining both the provisioning and every rollback failure.

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

## Verification

- `cargo test -p execution-storage -- --nocapture` — 20 passed; the real macOS
  APFS probe printed an explicit skip because quota mountpoint allocation lacks
  root privilege on this host.
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
- Destructive real APFS/LVM allocation tests require an operator-owned test pool.
  The current host supports the APFS read-only probe but is not running with the
  root privilege required for a quota mountpoint, so the destructive test is
  explicitly skipped.
- The Docker verifier is a neutral interface in this crate. A later runtime slice
  must implement daemon identity and bind-filesystem proof without creating a
  dependency cycle.
