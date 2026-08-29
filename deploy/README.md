# Single-host deployment

The supported first deployment runs PostgreSQL and the controller with Docker
Compose, and runs the execution worker on the host where APFS or thick LVM can
enforce disk reservations. The worker must not receive a raw host Docker socket.

1. Set `AUTOSPEC_POSTGRES_PASSWORD`, `AUTOSPEC_API_TOKEN`, and
   `AUTOSPEC_WORKER_TOKEN` to independent secrets. Set a stable
   `AUTOSPEC_DEPLOYMENT_ID`; every Compose-created resource is labelled with it.
2. Run `deploy/provision-storage-pool.sh plan apfs <probe-path> <state-root>` on
   macOS or `... plan lvm <volume-group> <state-root>` on Linux. `apply` is
   rejected unless `AUTOSPEC_STORAGE_CONFIRM` exactly matches the printed
   confirmation string. The script never creates or removes an APFS container,
   physical volume, or volume group; those destructive host operations remain
   an explicit operator responsibility.
3. Start the controller and the constrained Docker proxy with the exact stable
   project name and profile:
   `docker compose -p "$AUTOSPEC_DEPLOYMENT_ID" -f deploy/docker-compose.yml --profile host-docker-worker up -d postgres controller docker-api`.
4. Start `autospec-worker` on the storage host with the documented storage pool,
   immutable verifier image ID, `AUTOSPEC_WORKER_HOST_DOCKER=true`, and the
   constrained `tcp://127.0.0.1:${AUTOSPEC_DOCKER_PROXY_PORT:-2375}` Docker API
   endpoint already published by `docker-api`. The worker validates that the
   configured port matches and rejects non-loopback endpoints. A failed
APFS/LVM or Docker bind probe registers the worker Offline with a sanitized
`healthErrors` reason and zero advertised runtime capacity.

The proxy alone also joins `host-publish`, which lets Docker Desktop realize
the loopback publication. The worker is not attached to that network; its
container example reaches the proxy only through the internal `runtime-api`
network, and the supported host worker reaches only the loopback publication.

The optional `host-docker-worker` profile demonstrates container wiring through
an internal Docker socket proxy. It is disabled by default, never mounts the
socket into the worker, and still fails closed unless the container can prove a
real execution-storage pool. It is not a substitute for APFS/LVM host access.

Operator metadata is available through `autospecctl workers`, `executions`,
`queue`, and `cleanup-health`. The CLI uses `AUTOSPEC_API_TOKEN`, caps responses
at one MiB, and never requests artifact bodies, manifests, or task packets.

Podman and Apptainer are detected by the frozen
`autospec.dev/runtime-conformance/v1` report. They remain fail-closed for
provisioning until their adapters can satisfy the same durable storage,
resource-limit, ownership-label, and targeted-cleanup invariants as Docker.
