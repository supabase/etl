# Controlled Snowflake experiments

`etl-experiments` runs controlled replication workloads against a managed staging PostgreSQL source, a hosted ETL pipeline and a Snowflake destination. Each run has a saved plan, isolated source and destination schemas, finite write stages, an independent observer and correctness checks.

For deployment, credentials, Kubernetes operations and artifact export, see the [staging runbook](https://github.com/supabase/etl-k8s/blob/main/etl-experiments/README.md).

## What a run does

1. `prepare` creates a fresh UUID-named source schema and seeds all fixture rows before replication starts. It creates the publication and empty Snowflake schema, and records ownership in a source control table excluded from the publication.
2. `observe` starts separately. It samples source LSNs, retained WAL, transaction ages, logical decoding spill, effective settings, Platform table states, pipeline-scoped metrics, and the named replicator's image and resources.
3. `run` atomically claims the prepared fixture. A duplicate Kubernetes Job cannot replay it. By default it creates the destination and pipeline through **Platform**, selects an optional registered version, waits for the observer and starts the pipeline. Attach mode validates a stopped Studio-created pipeline against the fixture and destination.
4. The pipeline copies the prepared rows to Snowflake. After copying is observed, `copy_load` modifies source rows concurrently with initial synchronization. The runner waits for both that write stage to finish and every table's durable state to become `Ready`; an errored table fails the experiment. It then executes `cdc_loads` sequentially with configured aggregate operation rates, writer counts and transaction sizes.
5. Writers stop. A reserved row in every table receives a revision-only update, preserving its original large text value. Final marker rows are inserted. The runner waits for markers, then compares **every row and field** with each Snowflake `_CURRENT` view using bounded ordered pages. It distinguishes SQL NULL from JSON null. Source payloads never enter reports or errors.
6. The pipeline is stopped and deleted to reclaim its replication slots. Source fixtures, Snowflake data and artifacts remain for inspection. The report records stop and reclamation separately; unsuccessful teardown is never treated as a pass. `cleanup` removes the remaining owned API destination and, with `--drop-data`, the synthetic schemas.

The runner and observer are separate Kubernetes Jobs with separate persistent volumes. The observer requests cancellation and stops/deletes the owned pipeline on runner-heartbeat loss or a WAL budget breach. Neither process can guarantee cleanup during a cluster, source, or Platform outage: an unconfirmed teardown requires operator inspection. Sampling budgets can overshoot by writes made between observations; they are not a hard WAL or Snowflake billing cap.

## Fixture preparation and reuse

`prepare` and `run` are separate commands. Preparation generates and inserts the entire initial dataset once. Execution uses those existing source tables for initial sync, concurrent writes, CDC stages and final verification; it does not seed them again between stages. Preparation can finish before the runner and observer Jobs are started.

The loader generates deterministic text in Rust and sends parameterized `insert ... select ... from unnest(...)` batches. Tables load sequentially, with at most 256 rows and 1 MiB of generated text per batch. Each batch commits independently, followed by `analyze` after each table. Preparation has bounded batch memory but no configurable parallelism, `COPY` import or cached seed dataset.

The checked-in example expands to:

| Profile | Initial rows | Initial text payload |
| --- | ---: | ---: |
| Smoke | 1,024 | 16 MiB |
| Example baseline | 3,200,000 | 25.02 GiB |
| Double of example baseline | 6,400,000 | 50.05 GiB |

These are logical text sizes before CDC writes. They exclude JSONB, row/index/TOAST overhead and WAL, and do not predict compressed physical storage or Snowflake billing. Preparation precedes the run's generated-WAL measurement; it still produces source writes and WAL. Initial synchronization transfers the prepared dataset to Snowflake, and final verification reads every source and destination value again.

Every newly derived plan creates fresh source and Snowflake schemas and pays the full preparation cost. Completed source fixtures remain until explicit cleanup, but a prepared run can be claimed only once. Repeating `run` against completed data, importing an existing schema and resetting a retained fixture for another run are unsupported. A retained schema is evidence for inspection, not a reusable template.

## Three rollout levels

The checked-in `specs/baseline.json` is an **illustrative starting distribution, not measured customer data**. Calibrate a private copy before calling a run representative.

| Profile | Tables / volume | Purpose |
| --- | --- | --- |
| Smoke | 8 tables, 128 rows each, 16 KiB external text | Check authentication, fixture creation, initial sync, CDC, correctness, observation, report export and cleanup. |
| Baseline | Private calibrated groups; example has 304 tables | Reproduce the measured workload shape and sustained load. |
| Double | Exactly twice each group's table count and twice each stage's operations | Double initial text volume and CDC work while preserving the distribution and **rate**. It runs longer; increase the rate explicitly for a throughput-pressure experiment. |

Every derived plan gets a new run ID and namespace. Double must derive from the same calibrated **baseline** plan, not from smoke or a previous double. Resource/WAL/time budgets remain explicit and do not silently double.

```sh
ETL_SCCACHE=1 cargo x experiment plan --template /private/path/baseline.json --profile smoke --output /private/path/smoke.json
ETL_SCCACHE=1 cargo x experiment plan --template /private/path/baseline.json --profile baseline --output /private/path/baseline-run.json
ETL_SCCACHE=1 cargo x experiment plan --template /private/path/baseline.json --profile double --output /private/path/double-run.json
```

Do not run the three profiles automatically in sequence. Inspect each report and fix harness problems before advancing. An underachieved load yields **inconclusive**, even if the final data matches. This avoids calling a saturated generator a successful high-load replication test.

## Workload contract

Each table has a bigint primary key and revision, nullable text, and nullable JSONB. Groups specify table count, initial rows, text size, external TOAST storage, and DEFAULT or FULL replica identity. The generated payload varies by seed, table, row, revision and block. Autovacuum remains enabled.

Each writer owns disjoint key ranges. A deterministic cycle updates only revision, replaces text, clears text, changes JSON (including JSON null), deletes, and reinserts. The copy stage waits for observed copying and requires a committed source write while copying is still visible. A copy too fast to observe makes the result inconclusive. CDC begins only when every table's current durable state is `Ready`; the dashboard's broader following-WAL status is not sufficient. Transactions are paced after commit without catch-up bursts. Reports record requested/achieved operations per second, committed operations, affected rows, written text bytes and elapsed time. Operation count is not WAL volume: observed WAL is recorded separately. Concurrent scheduling is not bit-for-bit deterministic despite a fixed seed.

The runner creates and owns its fixture tables. Configure table groups from schema, size and transaction statistics; all row values are synthetic.

## WAL-retention cases

The default `retention: {"kind":"none"}` runs without an additional retention scenario. Other cases use fresh run IDs:

- `{"kind":"held_transaction","release_after_wal_bytes":67108864}` opens a write transaction **after initial sync**, while independent writers commit. It checks whether confirmed flush progresses past the transaction's start while restart LSN remains behind it, then commits the held transaction after the measured WAL threshold. If the workload does not produce enough WAL, the run fails with that explicit reason. Missing pinning evidence makes the result inconclusive. A passing result establishes the configured transaction's retention behavior.
- `{"kind":"large_transaction","rows":10000}` updates the reserved first table in one transaction before normal CDC stages. Its rows must fit that table. Use the observer to distinguish destination acceptance, commit/checkpoint progress and retained WAL.

The runner samples effective PostgreSQL settings throughout execution. Retention cases leave managed WAL settings unchanged; they do not exercise configuration changes or recovery from slot invalidation.

## Platform and Studio launch

Default: `"launch":{"mode":"platform"}`. The runner uses the project-scoped `/platform/replication/<project-ref>` endpoints, preserving Platform validation and the ordinary ETL API/runtime lifecycle. It does not embed a replicator or start an in-process pipeline.

For Studio: prepare first, then create a Snowflake destination/pipeline using the exact generated schema and publication. **Studio's current creation flow automatically starts replication. Stop that pipeline in Studio before attaching.** Set `launch` to `{"mode":"attach","pipeline_id":123}` in the private plan, then render/run it. Explicit `pipeline_config` fields must match the existing pipeline; use `{}` to accept its defaults and review the recorded effective configuration before comparing runs. The fixture fingerprint intentionally excludes launch mode so attachment is possible. Do not change the fixture groups after preparation. The runner owns resuming, stopping and ultimately deleting this dedicated experimental pipeline.

Attachment validates the resumed workload and final contents, but cannot retroactively observe initial synchronization. Its summary records `initial_sync_observed_from_start: false` and the whole-experiment verdict remains **inconclusive**, even when its observed behavior checks pass. Use automated Platform launch for smoke/baseline/double qualification from a fresh initial sync.

`version_id: null` uses the current default replicator version. A registered non-default image requires the experiment tenant in the API's `simulator_tenants` allowlist. Registering an image or changing that configuration is a separate reviewed staging operation. Reports record the selected API version and observed container image IDs; the runner image and replicator image are independent.

## Authentication and limits

Platform routes accept Studio JWTs, not PATs. The runner uses a **dedicated staging account with password sign-in and access to the test project**. Each process obtains its own JWT and renews before expiry with a new password grant. No session refresh token is shared between processes. Do not use a personal SSO/MFA account or paste a browser token for a long run. If staging does not permit this test-account setup, resolve that prerequisite before running; the harness does not bypass authentication.

Credentials come only from environment variables / mounted files:

- `EXPERIMENT_STAGING_PROJECT_REF`: must exactly match the plan.
- `EXPERIMENT_PLATFORM_URL`: full staging Platform replication endpoint, including project ref. Production `api.supabase.com` is rejected; operators must still supply the correct staging host.
- `EXPERIMENT_AUTH_URL`, `EXPERIMENT_AUTH_API_KEY`, `EXPERIMENT_PLATFORM_EMAIL`, `EXPERIMENT_PLATFORM_PASSWORD`: staging Studio Auth base (ending `/auth/v1`), its publishable/anon key, and dedicated test account.
- `EXPERIMENT_SOURCE_URL`: direct PostgreSQL URL, with the exact `source_host`; TLS VerifyFull is enforced. Use `sslrootcert=/credentials/source-ca.pem` when the project needs its supplied CA.
- `EXPERIMENT_SNOWFLAKE_PRIVATE_KEY_FILE`: mounted PEM key; an encrypted key additionally needs `EXPERIMENT_SNOWFLAKE_KEY_PASSPHRASE`.
- `EXPERIMENT_METRICS_URL`: staging Prometheus-compatible query base, supplied by infrastructure.

Plans contain identifiers but no credentials. Keep real plans and artifacts outside Git: treat identifiers, resource inventories and operational observations as private. The workload itself is synthetic.

Each load stage has a fixed target rate and finite operation count. More nuanced burst schedules can be represented as successive stages. Match source compute/storage, Postgres version, WAL/slot limits, replica identity, table-size and TOAST distributions, transaction sizes, hot-table distribution, replicator resources, Snowflake role/warehouse and regions before drawing conclusions about production. Current profiles distribute operations evenly across tables; workloads dominated by a few hot tables need additional calibrated scheduling before equivalence can be claimed.

## Reports and interpretation

Recover `runner/run` and `observer/observe`, then:

```sh
ETL_SCCACHE=1 cargo x experiment report --artifacts /private/path/results/runner/run /private/path/results/observer/observe --output /private/path/results/report.html
```

Open `report.html` locally. It requires no hosted dashboard or external JavaScript. `report.json` contains the combined verdict; original JSONL files preserve the full evidence. The HTML includes WAL/acknowledgement charts, phase chronology, workload rates, per-table verification and expandable observations. Metrics are retained in raw JSONL; the HTML summarizes their availability to remain manageable.

- **Passed:** all content checks passed, requested rates were achieved, observer evidence is complete and pipeline reclamation was confirmed.
- **Failed:** a source/destination operation or behavioral assertion failed. Inspect the last successful phase and error, then the corresponding source/Platform observations.
- **Inconclusive:** missing observations, insufficient achieved load, missing held-transaction evidence, or unconfirmed teardown. Fix the measurement/setup issue and use a fresh run ID.
- **Interrupted:** deadline, operator signal or watchdog cancellation. It is not a pass.

Never interpret matching final row counts alone as correctness. The harness compares values, including preserved TOAST payloads and JSON/SQL null semantics. Missing/partitioned Snowflake result pages cause an error rather than a false pass. The runner also stops if the independent observer heartbeat disappears. A failed critical source sample stops the run; client-side timeouts keep source outages from disabling watchdogs. Each load stage needs a source/metrics collection wholly inside its start/end boundaries, so sparse or delayed observations cannot silently qualify an unmeasured stage. Slot absence is verified against PostgreSQL after pipeline deletion. Unknown outcomes from create requests are not retried blindly; the source ownership record and exact publication/destination IDs support reconciliation.

## Local validation

Unit tests cover plan scaling, invalid plans, duplicate execution evidence, provider-result completeness, aggregate verdicts and staging rendering. The ignored PostgreSQL test requires a disposable **local** database; it rejects non-loopback hosts. It exercises real DDL, fixture ownership, load generation, cancellation, source sampling and the unchanged-TOAST probe.

```sh
RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0 SCCACHE_CACHE_SIZE=20G cargo nextest run --locked -p etl-experiments
# With EXPERIMENT_TEST_DATABASE_URL pointing to a disposable local PostgreSQL 17 instance:
RUSTC_WRAPPER=sccache CARGO_INCREMENTAL=0 SCCACHE_CACHE_SIZE=20G cargo nextest run --locked -p etl-experiments --run-ignored all
```

These tests do not prove hosted Platform authentication, staging networking or Snowflake behavior. The first staging smoke run is the gate for those integrations.
