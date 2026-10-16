# Single-host deployment

The supported first deployment runs PostgreSQL and the controller with Docker
Compose, and runs the execution worker on a Linux host where thick LVM can
enforce disk reservations. The worker must not receive a raw host Docker socket.

APFS allocation, quota, mount, exhaustion, and cleanup are physically tested as
a storage lifecycle on macOS. APFS is not currently a supported Docker worker
pairing: Docker Desktop's user-scoped file-sharing process cannot traverse the
required root-owned `0700` execution filesystem. The worker rejects every
pairing except Linux plus thick LVM during preparation. Supporting macOS Docker
requires a typed privilege-separated APFS helper; weakening permissions or
granting an unauthenticated ACL is not supported.

1. Set `AUTOSPEC_POSTGRES_PASSWORD`, `AUTOSPEC_API_TOKEN`, and
   `AUTOSPEC_WORKER_TOKEN` to independent secrets. Set a stable
   `AUTOSPEC_DEPLOYMENT_ID`; every Compose-created resource is labelled with it.
2. Run `deploy/provision-storage-pool.sh plan lvm <volume-group> <state-root>`
   on Linux. The APFS form remains available only for storage-lifecycle testing.
   `apply` is
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

Worker credentials. The controller refuses to start unless exactly one worker
credential source is configured:

- Per-worker tokens (recommended): `AUTOSPEC_WORKER_TOKENS_FILE` (or
  `--worker-tokens-file`) names a file with one `<worker_id> <sha256-hex-of-token>`
  per line (`#` comments). Issue each worker its own random token, store only
  the SHA-256 digest in the file (`printf %s "$TOKEN" | sha256sum`), give the
  worker the token as its `AUTOSPEC_WORKER_TOKEN`, and `chmod 600` the file; a
  file readable by group or other fails startup. A token is accepted for its own
  worker only. Do not also set the controller's `AUTOSPEC_WORKER_TOKEN`.
- One shared token (legacy): set `AUTOSPEC_WORKER_TOKEN` and
  `AUTOSPEC_ALLOW_SHARED_WORKER_TOKEN=true` (`--allow-shared-worker-token`). The
  shared token speaks for every worker, so a leak exposes all of them.

Upgrading: a controller started with only `AUTOSPEC_WORKER_TOKEN` now exits at
startup naming both options above instead of silently accepting the shared
token. The bundled `docker-compose.yml` passes
`AUTOSPEC_ALLOW_SHARED_WORKER_TOKEN` (default `true`, because that file already
requires the shared `AUTOSPEC_WORKER_TOKEN`). To move off the shared token, edit
the `controller` service: drop its `AUTOSPEC_WORKER_TOKEN` and
`AUTOSPEC_ALLOW_SHARED_WORKER_TOKEN` lines, mount the tokens file read-only, and
set `AUTOSPEC_WORKER_TOKENS_FILE` to its in-container path (the file must be
mode 0600 and readable by the controller's user).

Agent network isolation. By default every execution runs on an internal Docker
network with no route out. Model traffic goes through a per-execution relay
container that forwards only to one InferWeave `host:port`, and the agent
reaches it as `INFERWEAVE_RELAY_URL=http://inferweave:<port>` (point Pi's
provider base URL at that URL in the agent image). The worker therefore refuses
to start unless it is given the relay:

- `AUTOSPEC_INFERENCE_UPSTREAM` (`--inference-upstream`): the InferWeave
  `host:port`, for example `gateway.local:8080`.
- `AUTOSPEC_INFERENCE_RELAY_IMAGE` (`--inference-relay-image`): an image you
  supply (for example one carrying `socat`) whose entrypoint receives exactly
  `<listen_port> <upstream>` and forwards to the upstream. It must be stateless
  (no image volumes).
- `AUTOSPEC_INFERENCE_RELAY_PORT` (`--inference-relay-port`, default 8080).

To keep the previous behaviour (ordinary network, unrestricted agent egress) pass
`--allow-agent-egress` / `AUTOSPEC_ALLOW_AGENT_EGRESS=true` instead; it is logged
as a warning at startup and cannot be combined with the relay flags. Upgrading
without either leaves the worker exiting at startup with a message naming all
three flags. On an internal network, service containers (`postgres`, `redis`,
...) also lose outbound access; they remain reachable by the agent. The bundled
`docker-compose.yml` worker passes `AUTOSPEC_ALLOW_AGENT_EGRESS` (default `true`)
so the example deployment keeps starting; set it to `false` and add the two relay
variables to adopt the relay.

Agents run as a non-root user by default (`AUTOSPEC_AGENT_USER`, default
`1000:1000`). The execution directories are owner-only on the host, so a
non-root agent works only where the Docker daemon maps that uid to the worker's
owner (a rootful daemon running the worker as the same uid). Where it does not
(for example a rootless daemon), provisioning fails closed with a message
naming the mounts. Run as root there only by explicit choice: set
`AUTOSPEC_AGENT_USER=0:0` and `AUTOSPEC_ALLOW_ROOT_AGENT=true`.

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
