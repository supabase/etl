# Hostname connection regression lab

This opt-in fixture runs real DNS, IPv6, TLS, and PostgreSQL connections on an
isolated Docker network. It never changes host DNS, publishes database ports, or
mounts the Docker socket into a container. Only the disposable database container
receives `NET_ADMIN`, for its own address and packet-filter controls.

The same PostgreSQL instance listens at two synthetic IPv6 addresses. Tests change
DNS, reject the old address, and close its existing sockets without recreating the
database. Administrative setup uses a separate stable hostname. Generated test
certificates and private keys belong in the temporary lab directory, not Git.

Normal runs use the Git revisions and lockfiles in the selected workspaces.
The API's six ETL pins must stay aligned; SQLx patches must appear in both
workspace roots because Cargo does not inherit dependency patches.
Unpublished changes can be evaluated with an explicit Compose override mounting
companion worktrees and a Cargo patch config at `/usr/local/cargo/config.toml`.
Set `NETWORK_COMPOSE_OVERRIDE` to that host-side Compose file. Prepare matching
validation lockfiles first and preserve the originals. Keep machine-specific paths
outside Git. An overridden run validates that candidate, not the published graph.

Run from either host workspace, with the two test modules present:

```bash
export ETL_WORKSPACE=/absolute/path/to/etl-tests
export API_WORKSPACE=/absolute/path/to/api-tests
export NETWORK_LAB_DIR=/absolute/path/outside/git/network-lab
export DOCKER_CONTEXT=your-local-context
bash "$ETL_WORKSPACE/scripts/network-tests/run.sh" start
bash "$ETL_WORKSPACE/scripts/network-tests/run.sh" test
bash "$ETL_WORKSPACE/scripts/network-tests/run.sh" ipv4-first
bash "$ETL_WORKSPACE/scripts/network-tests/run.sh" down
```

Set `NETWORK_TEST_SUITE=etl` or `NETWORK_TEST_SUITE=api` to run only that
workspace's tests; the default is `both`. ETL-only runs do not require
`API_WORKSPACE`. Each repository's CI runs its own suite, including the forced
IPv4-first pass. API CI checks out the shared fixture from the same ETL revision
as its dependencies, so that revision must include `scripts/network-tests`.
The lab uses a local Docker daemon with IPv6 network support; CI uses Ubuntu 24.04.

Both suites are opt-in ignored integration tests. They run serially because they
change a shared fixture. The API suite runs even if an ETL assertion fails. Each
run writes a log under `NETWORK_LAB_DIR`; a failing assertion exits nonzero.
`start` generates a disposable CA and a two-day server certificate. For a later
run after expiry, use a fresh artifact directory. `down` removes this lab's
containers and network, while retaining build caches. It does not touch other
Postgres containers. Workspaces are mounted read-only, and compilation uses
named Docker volumes with sccache enabled.
`start` waits for the final PostgreSQL TCP listener and recreates the tester, so
server rebuilds cannot leave stale IPv6 neighbor entries in a retained client.
DNS-change scenarios themselves reuse the same running processes and network.
The API build uses the Rust image's system C linker instead of its repository's
clang/lld link-speed optimization. Debug symbols are omitted to keep disposable
build artifacts smaller; debug assertions and test behavior are unchanged.

The default pass observes Linux resolver ordering. The `ipv4-first` pass changes
only the tester container's `gai.conf` and asserts that IPv4 really appears first.
Both passes exercise IPv4-only and healthy dual-stack connections, immediate
rejection, and silently dropped packets in either address family. Successful
fallback must finish in less than two seconds. Separate tests use a 35-second
observation deadline for complete outages and stalled DNS, without shortening
the clients' own budgets. The IPv6-only pipeline test also covers fresh DNS
resolution after an endpoint change.

The pipeline test checks copied rows, CDC before and after endpoint retirement,
new backend identities, reuse of the SQLx pool, and a fresh persistent-store
reload. TLS and pin controls require valid connections to work, wrong hostnames
and untrusted roots to fail, and explicit IPv4/IPv6 pins to select an address
different from DNS. A blocked pin must fail even when DNS offers a healthy
alternative. The TLS-stall test checks the full connection deadline and socket
closure; query cancellation must target the address that won the TCP race. The API
test exercises real HTTP source creation and publication listing, including
omitted/null `hostaddr`, DNS changes, TLS rejection, and a stale pin.

This is a same-database endpoint-change test, not a PostgreSQL promotion test or
an assertion about a deployed cluster's DNS TTL, routing, or egress policy. API
Kubernetes interactions use the existing mock; source queries and TLS are real.
The driver worktrees contain deterministic paused-clock tests for stagger timing,
slow first-attempt success, bounded concurrency, later-address progress,
cancellation, simultaneous winners, and exhausted address lists.
