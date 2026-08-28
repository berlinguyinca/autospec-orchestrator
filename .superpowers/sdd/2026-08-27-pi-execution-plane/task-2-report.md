# Task 2 Report: storage-backed Docker runtime

## Outcome

`DockerRuntime` now provisions only from an exact live Ready
`execution-storage` allocation. Agent and service containers use immutable root
filesystems, disabled Docker logging, and deterministic writable bind directories
inside the allocation's aggregate hard-quota filesystem. Legacy constructors
remain available for daemon probes, reconciliation, and cleanup, but provisioning
through them fails closed.

## Runtime integration

- Added `connect_with_execution_storage`, consuming an exact
  `AllocationReceipt` and a live `ReadyAllocationVerifier` without changing the
  frozen `Runtime` trait.
- Provisioning exact-matches allocation ownership labels and `disk_gib`, verifies
  the durable Ready journal/backend/mount/Docker proof through the capability,
  pins that capability through provisioning, and revalidates it before Docker
  mutation.
- The runtime compares the current Docker daemon's immutable daemon ID with the
  allocation's daemon-derived bind proof before creating a network or container.
  A foreign-daemon receipt fails as `RuntimeError::ResourceLimit`.
- The agent receives the verified `layout.repository` at `/workspace` and only
  `layout.session/conversation` at `/session`. Host-private session siblings and
  the Docker socket are not mounted.
- Agent and service containers set `ReadonlyRootfs=true` and log driver `none`.
  No `StorageOpt`, tmpfs, anonymous volume, named volume, or published host port
  is used as quota authority.
- Every intentional writable location is a read-write bind beneath the one
  verified execution filesystem: deterministic per-container home, `/tmp`,
  `/var/tmp`, `/run`, and remapped image-declared `VOLUME` paths. Bind sources
  are canonicalized and created as direct descendants of `layout.runtime`.
- Image `VOLUME` declarations equal to, nested beneath, or shadowing
  `/workspace` or `/session` remain rejected before Docker resource creation.
- After container creation, daemon inspection must report readonly rootfs, log
  driver `none`, bind-only mounts, and canonical sources strictly below the
  execution root on the same host filesystem. After start, a daemon-side `stat`
  proves every read-write mount resolves to one device. Any failure rolls back
  through the execution-label selector.
- Existing API 1.41 negotiation, 404-only image pulls, execution-labelled object
  creation, aggregated rollback/cleanup diagnostics, selector-only destruction,
  and read-only reconciliation remain intact.

## TDD and real-Docker evidence

- A new red regression first showed that a foreign-daemon receipt could provision
  successfully. The runtime now rejects it before a network exists; the same test
  proves legacy provisioning fails closed.
- Real Docker inspection proves exact `/workspace` and `/session` sources,
  deterministic runtime bind sources, `ReadonlyRootfs=true`, log driver `none`,
  no tmpfs/storage options/volumes/socket/ports, and one daemon-observed device.
- Container writes prove `/workspace`, `/session`, home, temp, run, and Redis
  `/data` are writable and durable after container destruction. Writes to `/etc`
  fail for both agent and service roots. Private session metadata remains
  inaccessible.
- Real images declaring `/workspace`, `/session/history`, and `/` are rejected
  before execution resources appear while another execution remains running.
- The operator-configured lifecycle allocates two independent APFS/LVM execution
  filesystems, alternates 64 MiB writes across one execution's agent and service,
  accepts only ENOSPC after substantial successful writes, proves the other
  execution remains writable, then selector-cleans Docker resources and releases
  both exact storage receipts. It runs only when `AUTOSPEC_APFS_PROBE_PATH` or
  `AUTOSPEC_LVM_VOLUME_GROUP` is configured.

## Verification

- `cargo test -p runtime-docker -- --nocapture` — 9 unit and 15 integration tests
  passed against the real Docker daemon. The destructive aggregate quota test
  printed an explicit skip because no operator APFS/LVM pool is configured.
- `cargo fmt --all -- --check`, `cargo build --workspace`, `cargo test
  --workspace`, and `cargo clippy --workspace --all-targets -- -D warnings` —
  passed on the current toolchain.
- `cargo +1.85.0 fmt --all -- --check`, `build --workspace`, `test --workspace`,
  and `clippy --workspace --all-targets -- -D warnings` — passed; all 15
  runtime-docker integration tests passed under Rust 1.85 with the same explicit
  operator-pool skip.
- `git diff --check` — passed.

## Remaining concern

- This workstation has no configured destructive APFS/LVM worker pool, so the
  end-to-end aggregate ENOSPC test compiled and explicitly skipped rather than
  performing allocation. The execution-storage backend's own configured
  lifecycle test has the same operator gate. Production provisioning fails
  closed when the live Ready allocation or its daemon bind proof is unavailable.
