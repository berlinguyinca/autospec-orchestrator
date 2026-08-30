# Linux thick-LVM aggregate-exhaustion proof handoff

## Purpose

Close the one remaining Task 9 evidence gate on a disposable Linux host with a
native Docker daemon and a real thick-LVM volume group. The existing test must
fill one execution across its agent and service paths until the filesystem
returns ENOSPC, then prove a peer execution remains writable and the shared
mirror sentinel is unchanged.

The commit containing this handoff deliberately rejects macOS APFS plus Docker
Desktop during worker preparation. APFS lifecycle and quota enforcement passed
physically on macOS; Docker Desktop cannot traverse the required root-owned
`0700` execution root. Do not weaken permissions or add ACL exceptions.

## Host prerequisites

- Linux with Rust 1.85 or newer and this exact branch commit checked out.
- A native, reachable Docker daemon. Do not use Docker Desktop file sharing.
- `lvm2`, `mkfs.ext4`, `mount`, and root privileges.
- A deliberately disposable thick-LVM volume group with at least 4 GiB free.
  Do not use a production VG. Thin pools are not accepted.
- `alpine:3.20` and `redis:7-alpine` must be pullable or already present.

Record before testing:

```bash
git rev-parse HEAD
uname -a
rustc --version
docker version
sudo /usr/sbin/lvm vgs --units b --nosuffix \
  --options vg_name,vg_uuid,vg_size,vg_free <DISPOSABLE_VG>
```

## Provision and probe

Choose an owner-only state root on the host. The script never creates or
removes the VG or its physical volume.

```bash
export AUTOSPEC_LVM_VOLUME_GROUP=<DISPOSABLE_VG>
export AUTOSPEC_STATE_ROOT=/var/lib/autospec-proof

deploy/provision-storage-pool.sh plan lvm \
  "$AUTOSPEC_LVM_VOLUME_GROUP" "$AUTOSPEC_STATE_ROOT"

export AUTOSPEC_STORAGE_CONFIRM="provision:lvm:${AUTOSPEC_LVM_VOLUME_GROUP}:${AUTOSPEC_STATE_ROOT}"
sudo --preserve-env=AUTOSPEC_STORAGE_CONFIRM \
  deploy/provision-storage-pool.sh apply lvm \
  "$AUTOSPEC_LVM_VOLUME_GROUP" "$AUTOSPEC_STATE_ROOT"
```

## Required proof commands

Run both tests as root so LVM allocation, ext4 mounting, owner-only directory
capture, and native Docker bind verification share one host authority.

```bash
set -o pipefail
export CARGO_BIN="$(command -v cargo)"

sudo --preserve-env=AUTOSPEC_LVM_VOLUME_GROUP \
  env AUTOSPEC_LVM_VOLUME_GROUP="$AUTOSPEC_LVM_VOLUME_GROUP" \
  AUTOSPEC_STORAGE_TEST_BYTES=16777216 \
  "$CARGO_BIN" test -p execution-storage --test backends \
  configured_real_pool_runs_full_quota_lifecycle_or_explicitly_skips \
  -- --exact --nocapture |& tee /tmp/autospec-lvm-lifecycle.log

sudo --preserve-env=AUTOSPEC_LVM_VOLUME_GROUP \
  env AUTOSPEC_LVM_VOLUME_GROUP="$AUTOSPEC_LVM_VOLUME_GROUP" \
  "$CARGO_BIN" test -p runtime-docker --test docker_runtime \
  configured_storage_enforces_one_aggregate_quota_and_preserves_another_execution \
  -- --exact --nocapture |& tee /tmp/autospec-lvm-aggregate.log
```

Both commands must execute one test and report `ok`; an explicit `SKIP` is not
success. The aggregate test itself requires at least 256 MiB of successful
writes before ENOSPC, a successful peer write afterward, byte-identical shared
mirror sentinel content, and exact release of both storage allocations and all
owned Docker resources.

## Residue and evidence checks

Read-only inventory only; never use global Docker or LVM prune commands.

```bash
sudo /usr/sbin/lvm lvs "$AUTOSPEC_LVM_VOLUME_GROUP" \
  --options lv_name,lv_uuid,lv_tags,lv_path
docker ps -a --filter label=autospec.managed=true
docker network ls --filter label=autospec.managed=true
docker volume ls --filter label=autospec.managed=true
```

Do not delete resources discovered by these broad read-only inventories: they
may belong to another execution. The tests perform exact identity-checked
cleanup. If residue exists, preserve the logs and identities for diagnosis.

## Reconciliation after success

1. Copy the two complete logs and host metadata into the durable audit evidence
   location chosen by the operator; do not commit secrets or Docker endpoints.
2. Update `docs/architecture/phase-5.5-audit.md` with the commit, kernel, Docker,
   LVM VG UUID, test durations/results, ENOSPC evidence, peer/mirror evidence,
   and residue inventory.
3. Check the aggregate-exhaustion item in
   `docs/superpowers/plans/2026-08-27-pi-execution-plane.md` only after reviewing
   those logs.
4. Run the full current/MSRV workspace gates and obtain independent review
   before declaring Task 9 complete.
