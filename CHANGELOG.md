# Changelog

## [0.1.1] - 2025-09-03

### Bug Fixes

- *(ci)* Fix formatting (#319)
- *(ci)* Fix docker build (#317)
- *(postgres)* Ensure that table sync works with Postgres<=14 (#312)
- *(config)* Fix wrong parsing (#310)
- *(test)* Fix missing primary key test (#301)
- *(state)* Reset slot and table also on Init (#299)
- *(replication)* Fix apply loop event conversion ordering (#291)
- *(bigquery)* Allow missing table schema when truncate is issued
- *(api)* Make sure ETL tables are there before performing any action on the source etl schema (#286)
- *(api)* Skip serializing option config (#285)
- *(api)* Implement storable trait to make it more reliable to store configs in the db (#283)
- *(etl-api)* Allow creating empty publications
- Fix clippy warnings (#247)
- Fix dockerfiles and compile only etl-replicator with panic=abort (#243)
- Add missing project ref in logs (#230)
- Encoding bugs when sending to BigQuery (#210)
- *(tests)* Attempt at fixing stalling tests (#212)
- Tables with missing primary key fail with a proper error (#204)
- Fix a failing test (#207)
- *(core)* Improve edge cases of replication (#198)
- Clashes in routes (#199)
- Potential data corruption bug and improve message sending behaviour (#174)
- *(sentry)* Improve Sentry integration (#182)
- Make ci run faster (#179)
- Setting ENABLE_TRACING did not work (#178)
- Process_syncing_tables returned a wrong value (#168)
- Update replicator config (#164)
- Avoid unnecessary table sync worker launches (#166)
- Update database setup guide reference (#144)
- *(tests)* Attempt at fixing tests stalling (#143)
- Ignore tests which get stuck (#141)
- Bump max_replication_slots to 100 to make tests pass (#140)
- Clippy warnings
- Remove unneeded unit return types
- Fix incorrect file merges while rebasing
- Formatting
- Run for all features as well
- Fix a clippy warning
- Run cargo clippy for all targets in ci
- Clippy warnings
- Log panics via tracing
- Formatting
- Mount logs volume in both replicator and vector sidecar containers
- Fix failing tests
- Fix incorrect syntax in start_replication command
- Fix method docs
- Fix config in replicator

After the snake_case renaming, deserializing the settings from env variables was
failing.
- Fix casing at another two locations
- Fix casing of config objects
- Fix clash between routes

the POST /pipelines/{pipeline_id} and /pipelines/stop routes clashed with each
other resulting in /pipelines/{pipeline_id} being selected for a /pipelines/stop
url. Fixed by forcing {pipeline_id} to be all digits
- Fix prerequisites for running code coverage
- Fix #80 - error when using PipelineAction::CdcOnly
- Wrap bq column names in backticks
- Retrieve distinct columns
- Syntax error when getting column schemas
- Fix a couple of bugs in array parsing
- Set pipeline resumption state props public
- Fix #59: failure when replica identity full is set
- Fix SimpleQueryMessage handling after upgrade
- Fix time format in duckdb
- Fix formatting
- Fix missing timestamptz support in a few places in BQ sink
- Fix #52 false bool values are inserted as null in BigQuery
- Fix formatting
- Boolean parsing
- Fix quote_identifier impl
- Fix a bug for empty publications

If a publication didn't have any tables, it was not returned by the api.
- Fix postgres port in GH action
- Fix a clippy warning
- Fix clippy warnings
- Fix docker warnings
- Fix formatting
- Fix clippy warnings
- Fix docker warnings
- Fix clippy warnings
- Fix a clippy warning
- Fix failing test
- Fix a bug in which duckdb client incorrectly returned true from table_exists and schema_exists methods
- Fix inability to replicate to duckdb file
- Fix language for a sentence in README
- Fix command
- Fix sqlx cli installation
- Fix cargo formatting
- Fix perf bug: send all events at once instead of one by one
- Fix a bug in which batch wasn't ending on a keepalive message
- Fix bugs and improve logging

* fixed a bug in which truncate is now allowed in bigquery cdc enabled tables,
  so we now drop and recreate the table.
* fixed a bug in which default stream was not created when creating a table, we
  now wait for the stream to be created before moving on.
* fixed a bug in which cdc was not enabled on bigquery tables when doing initial
  table copy.
- Fix timer taking longer for the first time
- Fix missing committed lsn bug
- Fix a warning
- Fix clippy warnings/errors
- Fix a bug in which update event wasn't processed

### Features

- *(ci)* Implement mechanism to release new versions of ETL (#316)
- *(ci)* Split tests in CI in partial and full (#311)
- *(auth)* Add support for multiple api keys (#307)
- Add same labels to all replicators (#308)
- *(readme)* Add badges (#306)
- *(logs)* Add pipeline id to replicator logs (#304)
- *(etl)* Run CI on main (#305)
- *(state)* Delete previous state on rollback (#303)
- *(publications)* Handle tables removed from the publication (#300)
- *(agents)* Add AGENTS.md file (#292)
- *(bigquery)* Handle out of range values for BigQuery (#280)
- Add iceberg client (#279)
- *(bigquery)* Use the new concurrent append in BigQuery (#274)
- *(postgres)* Change connection defaults (#277)
- Add egress metric (#275)
- *(docs)* Write some docs (#270)
- Add metrics to replicator (#265)
- *(db)* Add created_at and updated_at columns in replication states (#266)
- Add metrics to the api crate (#257)
- *(branding)* Add new logo and update docs (#263)
- *(docs)* Write `etl` crate Rust docs (#262)
- *(replication)* Delete replication slots after pipeline is deleted (#260)
- *(ci)* Publish mkdocs to GitHub pages (#259)
- *(store)* Add new table mappings in the state store (#256)
- *(tests)* Add comprehensive tests for conversions (#254)
- *(bigquery)* Implement truncate with views (#253)
- *(store)* Add more supported types in Postgres schema (#252)
- *(bigquery)* Handle arrays with null values (#251)
- *(docs)* Write rust docs for some crates (#250)
- *(bigquery)* Reduce the amount of times a table is checked (#249)
- *(api)* Add new destinations pipelines delete endpoint and rework constraints (#246)
- *(logs)* Export project at top level field in the log (#245)
- *(api)* Enforce one default image (#241)
- *(api)* Perform deletion of state and schemas on pipeline delete (#239)
- *(bigquery)* Escape table names in BigQuery destination (#240)
- *(docs)* Improve READMEs (#233)
- *(api)* Improve endpoint for rollback (#232)
- *(api)* Add new endpoint for rolling back the state (#228)
- *(etl)* Implement new state in the state store (#227)
- Add update pipeline config endpoint (#219)
- *(openapi)* Use nesting in utoipa (#222)
- *(errors)* Rework how errors work from the ground up (#215)
- *(postgres)* Add sane defaults for the connection (#213)
- *(postgres)* Add new type for TableId (#214)
- *(api)* Return also table id from the status endpoint (#209)
- *(tests)* Split tests into multiple binaries (#206)
- *(bigquery)* Use LSN to discriminate between events with the same primary key (#201)
- *(api)* Implement new endpoint to send replication status (#203)
- Add bechmarking scripts (#205)
- *(pipeline)* Improve performance of the pipeline (#200)
- *(k8s)* Read Sentry DSN from secrets (#197)
- *(k8s)* Run vector as sidecar container (#196)
- *(logs)* Improve logs with spans (#194)
- *(api)* Expose replicator errors (#191)
- *(logs)* Improve logs (#192)
- *(api)* Implement update-image endpoint and improve consistency (#185)
- *(replicator)* Add SIGTERM handling (#188)
- *(observability)* Log top-level error (#184)
- *(observability)* Add sentry on the replicator (#180)
- *(ci)* Improve GitHub Actions (#176)
- *(docker)* Improve Docker build (#175)
- *(docker)* Improve docker images (#173)
- *(ci)* Add action for building docker images (#172)
- *(observability)* Add Sentry in API (#171)
- *(cleanup)* Implement replication slots cleanup (#165)
- *(api)* Do not return the stripped destination config (#170)
- Improve logs (#169)
- Remove duplicate method and simplify replication client (#167)
- Limit max number of table sync workers (#158)
- *(api)* Do not return sensitive fields in responses (#162)
- *(bigquery)* Implement BigQuery destination (#154)
- *(tests)* Add ability to show logs in tests for easier debugging (#156)
- Add postgres state store to allow state to be persisted to the source db (#152)
- *(security)* Add `Secret` to more fields (#151)
- *(replicator)* Add v2 pipeline in the replicator (#150)
- *(pipeline)* Implement table schema loading from the destination (#149)
- *(apply)* Implement CDC events streaming (#138)
- *(v2)* Add new replication client and implement table/schema copy (#136)
- *(v2)* Implement basic structure for new pipeline
- *(tests)* Add more integration tests
- *(tests)* Add integration tests for pg_replicate
- Add vector sidecar to replicator pods
- *(script)* Improve the script for setting up the database

### Miscellaneous

- Rename etl namespaces (#281)
- Improve (#278)
- Setup iceberg environment and other improvements (#276)
- Include response receive time also in metric (#273)
- Use Postgres instead of PostgreSQL (#272)
- Move all logos inside docs/assets folder (#268)
- Silence coveralls upload failures (#267)
- Update (#261)
- Add tests for TOAST values (#242)
- Update api binary name to etl-api (#244)
- Remove an unnecessary expect call (#235)
- Remove an unnecessary panic hook (#229)
- Use expect instead of allow (#223)
- Update benchmark scripts (#218)
- Fix cargo command in example (#193)
- Cleanup old code (#186)
- Use --no-deps flag and remove explicitly listed features (#189)
- Bump rust version to 1.88 in dockerfiles (#187)
- Use @supabase/etl group in codeowners (#190)
- Bump edition for all crates to 2024 (#163)
- Use TableId instead of Oid and rename a couple of structs (#155)
- Add content to the home page (#148)
- Configure mkdocs to generate documentation for the project (#147)
- Ensure only one pipeline can be created for one source and destination id pair per tenant (#139)

* add migration and update pipeline to ensure unique constraint per tenant_id, source_id and destination_id

* remove unnecssary as_derefer in is_duplicate_pipeline_error
- Merge pull request #135 from nbiscaro/fix-null-parse

Check for quotes before parsing NULL
- Check quotes
- Merge pull request #134 from supabase/riccardo/feat/workers-structure
- Reformat
- Merge remote-tracking branch 'origin/riccardo/feat/workers-structure' into riccardo/feat/workers-structure
- Propagate panic
- Improve
- Improve
- Improve
- Improve
- Improve
- Improve
- Improve
- Improve
- Improve
- Improve failure handling
- Add main method
- Improve
- Improve
- Add more components
- Add more structure
- Add more components
- Merge pull request #133 from supabase/rs/fix-links-in-readme

docs: correct links in readme
- Correct links in readme
- Merge pull request #131 from supabase/rs/sinks-to-destinations

rename sinks to destinations everywhere in the code
- Cargo fmt
- Rename sink to destination in module names
- Rename sink to destination everywhere in code
- Update sqlx data
- Rename sink_name to destination_name in db layer
- Rename a few aliases in sql
- Rename sink_id column of app.pipeline table to destination_id
- Rename app.sinks table to app.destinations
- Merge pull request #130 from supabase/rs/rename-supabase_etl-to-etl

rename supabase etl to etl
- Cargo fmt
- Rename supabase_etl to etl
- Merge pull request #128 from supabase/rs/rename-sink-to-destination

rename sink to destination
- Rename pg_replicate to supabase_etl
- Rename sink to destination everywhere except in db, api or config
- Rename sinks module to destinations
- Rename sink -> destination in readme files
- Merge pull request #126 from supabase/riccardo/feat/add-tests
- Fix
- Remove println
- Remove println
- Fix
- Fis
- Improve
- Merge pull request #125 from supabase/rs/run-clippy-for-all-targets

fix: run cargo clippy for all targets in ci
- Merge pull request #121 from supabase/riccardo/feat/add-integration-test
- Improve
- Make queries lowercase
- Fix PR comments
- Improve
- Improve
- Improve
- Improve
- Improve
- Trying to fix
- Fix
- Fix
- Setup wal level in ci
- Improve
- Update action
- Improve
- Reformat
- Remove code
- Improve
- Improve
- Implement wait mechanism
- Fix
- Improve
- Improve
- Add facilities
- Fix
- Improve
- Improve
- Improve
- Improve
- Add test facilities
- Use options in pg_replicate
- Implement tokio facilities
- Merge pull request #124 from supabase/rs/trace-panics

send panic logs to Logflare
- Merge pull request #123 from supabase/rs/enable-logs-volume

enable logs volume in replicator and vector sidecar containers
- Merge pull request #122 from supabase/rs/vector-logs

feat: add vector sidecar to replicator pods
- Merge pull request #118 from supabase/riccardo/ref/restructure-integration-tests

ref(tests): Improve integration tests structure and teardown behavior
- Explicitly name log
- Merge
- Merge
- Merge pull request #120 from supabase/codeowners

add @supabase/postgres team in codeowners
- Correct codeowners file folder
- Add @supabase/postgres team in codeowners
- Merge pull request #119 from supabase/emit-project-ref

emit project field in the root span in replicator
- Suppress the large variant warning for now
- Rename root span and function names
- Emit logs on closing span only from api
- Emit project field in the root span in replicator
- Remove unused code
- Format
- Add better message
- Improve tests
- Merge
- Merge pull request #117 from supabase/riccardo/feat/improve-script
- Update readme
- Add better message
- Improve readme
- Merge pull request #116 from supabase/emit-project-ref

emit project field in the root span in api
- Emit project field in the root span in api
- Remove .idea and .DS_Store files, update .gitignore
- Merge pull request #115 from supabase/improve-logging

Improve logging in the API and replicator crates
- Use correct log file name based on binary name (api or replicator)
- Use structured logging instead of string logging
- Extract telemetry related code into a separate crate

The new telemetry crate has common code for configuring telemetry and is reused
in both the api and the replicator binaries.
- Improve logging in the api

In a dev environment:

* Logs are printed on stdout
* Logs are pretty printed with colors enabled

In a prod environment:

* Logs are sent to a file in the logs folder
* Writing to the file is done on a background process for performance
* Logs are written in structured json format
* Logs are rotated daily to avoid filling up the disk
* Maximum 5 log files are kept, older are rotated out

Some multi-line logs lines have been flattened into a single file as they didn't
play well with the json output. The older bunyan format logger has been removed.
- Merge pull request #113 from supabase/cascade-delete-pipelines

Cascade delete pipelines
- Cascade delete pipelines when deleting sources/sinks
- Merge pull request #112 from supabase/update-sinks-pipelines

add `POST /sinks-pipelines/{sink_id}/{pipeline_id}` endpoint
- Update sinks-pipelines doesn't return ids
- Add test to check that cross tenant sink-pipeline's pipeline can't be updated
- Add test to check that cross tenant sink-pipeline's sink can't be updated
- Add test to check that cross tenant sink-pipeline source can't be updated
- Add the POST /sinks-pipelines/{sink_id}/{pipeline_id} endpoint to update a sink and a pipeline
- Merge pull request #111 from supabase/add-more-tests

add a new cross tenant pipeline creation test and fixed a bug it exposed
- Add a new cross tenant pipeline creation test and fixed a bug it exposed
- Merge pull request #110 from supabase/remove-dead-code

remove unused code
- Remove unused code
- Merge pull request #109 from supabase/sink-pipeline

add the POST /sinks-pipelines endpoint
- Add the POST /sinks-pipelines endpoint

This endpoint creates a sink and a pipeline in a single transaction.
- Merge pull request #108 from supabase/tenant-source

add the POST /tenants-sources endpoint
- Add the POST /tenants-sources endpoint

This endpoint creates a tenant and a source in a single transaction.
- Merge pull request #107 from supabase/del-test-dbs-command

move the code to delete test database into a command from a test
- Only delete databases with names as uuids

Test databases are named as random uuids, to reduce the risk of useful databases
being deleted, we now check that the database name is a valid uuid before
deleting it.
- Fully qualify a table name
- Move the code to delete test database into a command from a test
- Merge pull request #106 from supabase/fix-syntax-error

fix incorrect syntax in start_replication command
- Use quote_literal instea of raw single quotes
- Merge pull request #105 from supabase/chore-review-actions

ci: explicit permission in GH actions
- Additional GH Token permissions in test
- Explicit permission in GH actions
- Merge pull request #104 from supabase/tls

Enable TLS when a replicator connects to the database
- Fail fast with error when tls settings are incorrect
- Inject trusted root certs into replicator config
- Use dependency from fork instead of inlining code
- Remove unused dependency
- First working version
- Allow connecting with an ssl mode and a trusted root cert
- Propagate ssl mode to the postgres client
- Merge pull request #103 from supabase/all-features

pass --all-features to cargo clippy
- Pass --all-features to cargo clippy
- Merge pull request #102 from supabase/remove-delta

Remove delta support
- Revert "update code syncing with new version"

This reverts commit a36fa7b2b4dd6c2a04ced97c06b278e16e71a4b9.
- Revert "update code syncing with new version"

This reverts commit c31a5bd26048afadcedc94cb55bf09d1add15a3f.
- Revert "update code syncing with new version"

This reverts commit 056c791acebde15993f18d4bbb650ba22a004652.
- Revert "update code repair rust fmt issue"

This reverts commit 638cb8bf8696eda313100587f8f3b2f7f08e8b55.
- Revert "update code fix issue from author"

This reverts commit c9fa8a392ecf56c3943c5900a23ed139ac2381f3.
- Revert "update code fix issue from author"

This reverts commit 969b6bb22a3a688bf3cd50bf0f437a91413d6493.
- Revert "update code fix issue from author"

This reverts commit 4afda0151b6839908e8e13024a56a6457f6b6e14.
- Revert "update code fix issue from author"

This reverts commit b09ceb2c84bf47b804ca1332b57caf72dcbf81cb.
- Revert "update code fix issue from author"

This reverts commit 223bc445de188f603c97d1c03666d2135988edb9.
- Merge pull request #101 from supabase/add-truncate-support

add support for origin and truncate message types
- Add support for truncation in duckdb sink
- Add a comment explaining why truncate is not supported in bigquery
- Add support for origin and truncate message types
- Merge pull request #98 from supabase/update-advisories

Suppress some aggressive Rust security advisories
- Suppress some aggressive Rust security advisories
- Merge pull request #97 from supabase/bump-rustc-in-dockerfile

bump rustc version in dockerfiles to 1.85
- Bump rustc version in dockerfiles to 1.85
- Merge pull request #95 from supabase/configurable-max-staleness

Configurable max staleness
- Add max_staleness in api
- Make max_staleness configurable in pg-replicate and replicator
- Merge pull request #93 from supabase/update-deny-toml

removing a stale suppressed advisory
- Removing a stale suppressed advisory
- Merge pull request #92 from supabase/fix-config-deserialization

fix config in replicator
- Merge pull request #91 from supabase/correct-casing

fix casing of config objects
- Use snak case for all fields instead of renaming individual fields
- Merge pull request #88 from supabase/return-objects-not-arrays

API responses now always return objects and never arrays
- Wrap publications api response in an object
- Wrap tables api response in an object
- Wrap images api response in an object
- Wrap pipelines api response in an object
- Wrap sinks api response in an object
- Wrap tenants api response in an object
- Wrap sources api response in an object
- Merge pull request #87 from supabase/rename-api-objects

use snake case in naming api objects for consistency
- Use snake case in naming api objects for consistency
- Merge pull request #86 from supabase/delete-tenant-idempotent

delete tenant API always returns ok
- Delete tenant always return ok

even when the tenant doesn't exist. This is to make the api idempotenent and
make it easier to use from the worker code.
- Update sqlx metadata
- Add api endpoint to stop all pipelines
- Remove redundant test jobs
- Use llvm-cov for code coverage
- Add step to report coverage to coveralls
- Enable cascase delete on foreign keys referencing app.tenants.id column

This fixes a bug in which the delete tenant api fails because cascade delete was
not enabled.
- Correct and error message
- Simplify column list creation
- Merge pull request #85 from passcod/feat/colsubset

Support publication column lists
- Specify columns in the copy phase
- Literal-quote the publication
- Fmt
- Clippy
- Support publication column lists
- Bump gcp-bigquery-client version to 0.25.0
- Revert "temporarily logging for troubleshooting"

This reverts commit edec3e4ad5ac4b24b1d4ecacfd7b3b3ac235a08d.
- Temporarily logging for troubleshooting
- Bump rustc version in dockerfile to 1.83
- Add a troubleshooting section in README
- Only read database settings when running migrations
- Add a comment
- Remove row str logging
- Handle escape sequences
- Revert "log cdc events for troubleshooting"

This reverts commit 06c4e2eb1f84073e632dbb29928c832f618472e8.
- Revert "log table schemas for troubleshooting"

This reverts commit 37277c05070e91b105223a1623e3a87c84c98be7.
- Log table schemas for troubleshooting
- Log cdc events for troubleshooting
- Use default value for unchanged toast values
- Remove unused enum variant
- Merge pull request #75 from abduldjafar/feat/deltalake

Feat/deltalake
- Update code fix issue from author
- Update code fix issue from author
- Update code fix issue from author
- Update code fix issue from author
- Update code fix issue from author
- Update code repair rust fmt issue
- Update code syncing with new version
- Update code syncing with new version
- Update code syncing with new version
- Remove Sink trait and DataPipeline in favour of BatchSink and BatchDataPipeline
- Mark Sink trait as deprecated
- Merge pull request #66 from passcod/associated-error-types

Make sinks, sources, and pipelines generic on their errors
- Revert unrelated write_table_rows change
- Format
- Allow write_table_rows to handle errored rows itself
- Add infallible error variants for sinks and sources

those have to be separate, so we can't have both impls for std's infallible
- Make sinks, sources, and pipelines generic on their errors
- Do not send an append_rows request larger than 10 MB
- Remember final_lsn across batches
- Revert "add logging in bigquery sink"

This reverts commit a8abad5aef2aeb344924d8a599015d0a7fdfed5d.
- Add logging in bigquery sink
- Revert "add logging to help troubleshoot cdc failure due to commit msg without begin msg"

This reverts commit 00b9960420e0379dab69b75d03f9cd657532c1e9.
- Add logging to help troubleshoot cdc failure due to commit msg without begin msg
- Return null for unchanged toast values
- Ignore cdc events with missing schema error
- Log table id of skipped tables
- Revert "add logging to help troubleshoot"

This reverts commit 0cf109e0a3f6a1112f8152f290d30389087f1b8a.
- Order column schemas by attnum
- Filter out tables without primary keys
- Ensure consistent table copy order in BatchDataPipeline
- Add logging to help troubleshoot
- Remove an unknown lint
- Filter out generated columns
- Remove commented out code
- More fixes
- Handle more parsing cases
- Partially parse text format
- Try_from_str instead of try_from_bytes
- Rename bytes module to text
- Always use text format for CdcStream
- Prepare to always use text format in table copies
- Move conversion code into a separate module
- Factor out FromBytesError from CdcEventConversionError
- Only join with primary key indices when finding column schema
- Merge pull request #68 from supabase/deji/fix/use-backticks-for-cols

fix: wrap bq column names in backticks
- Ensure consistent table copy order
- Log column nullability in error
- Use VecWrapper instead of Vec
- Log error details when get_cell_value fails
- Merge pull request #65 from supabase/deji/fix/distinct-columns

fix: retrieve distinct columns
- Use <schema>_<table> format to name tables in BQ
- Merge pull request #64 from supabase/deji/fix/syntax-error-column-schemas

fix: syntax error when getting column schemas
- Merge pull request #63 from supabase/text-format

add support for parsing values from text format
- Parse json and bytea array types from text format
- Parse more array types from text format
- Parse bool array type from text format
- Parse date, time, uuid, json and oid types from text format
- Parse bytea type from text format
- Parse numeric types from text format
- Parse float types from text format
- Parse text types from text format
- Parse bool from text format
- Parse int2 and int4 from text format
- Parse int8 from text format
- Remove TextFormatNotSupported error
- Remove duplicate code
- Use text and binary format converters
- Do not use binary option if not supported by server
- Error out if replica identity is not default or full
- Merge pull request #61 from mfzl/fix/resumption-state-field-visibility

fix: set pipeline resumption state props public
- Do not run docker job on a PR
- Rename a method
- Rename a method
- Correct query syntax
- Add array type support in cdc stream
- Remove unused code
- Use binary format for CdcStream
- Remove commented out code
- Add array type support in table_row
- Add support for array types
- Bump gcp-bigquery-client version
- Bump gcp-bigquery-client version
- Bump futures version
- Update cargo deny config
- Update kube versions
- Use a raw string literal
- Upgrade to latest rust-postgres
- Default to string type instead of bytes
- Add oid support
- Add jsonb support
- Add json support
- Add uuid support
- Add time support
- Add date support
- Add bytea support
- Use bignumeric instead of of numeric in bigquery
- Add support for numeric type
- Use correct type
- Use gcp-bigquery-client version with double type support
- Bump gcp-bigquery-client version to 0.24.1
- Add support for float types
- Better error handling in get_cell_value
- Timestamptz is correctly converted into DateTime<Utc>
- Use pg_escape crate
- Use types other than String in Cell::TimestampX types
- Remove unnecessary formatting and parsing
- Add support for timestamptz for cdc events
- Use DateTime<Local> instead of DateTime<Utc>
- Add timestamptz support for table copy
- Return error from PG source if publication doesn't exist
- Derive debug on PipelineAction struct
- Add a feature to enable/disable unknown type to byte conversion
- Add comments to keep conversion impls in sync
- Move Cell enum to conversions module
- Create app schema
- Remove duplicate code
- Update sqlx metadata
- Remove background task queue
- Remove status from replicator
- Merge pull request #48 from WcaleNieWolny/fix_boolean_values

fix: boolean parsing
- Fix lint
- Update sqlx metadata
- Return source and sink names in pipeline apis
- Update sqlx metadata
- Add names to sources and sinks
- Remove migration and inline its changes in earlier migrations
- Update sqlx metadata
- Add create or update tenant api
- Update tenant id type to a string in db

* also allow sending the tenant id in request
- Change default pg port to 5430 to avoid conflicts with infra
- Convert unsupported types into bytes

Instead of throwing an error which exits the process, we now convert cdc events
containing unsupported types into bytes.
- Add api endpoint to get pipeline status
- Print type name in 'unsupported type' error
- Do a constant time comparison of api keys
- Add api_key authentication
- Use namespaced api
- Use replicator pods in a separate namespace
- Base64 encode secrets
- Add a log line when migrations run successfully
- Use sqlx version 0.8.2
- Add a migrate command to migrate the db
- Add a todo
- Correct api paths in openapi docs
- Remove cli crate
- Add openapi docs for all endpoints
- Temporarily disable rust advisory
- Enable reqwest feature in utoipa-swagger-ui dep
- Add openapi docs for /tenants endpoints
- Encrypt sink service account key
- Make password optional again
- Encrypt source password
- Prepare to encrypt data in db
- Removed a couple of todos
- Move start/stop pipeline api calls from background worker to the api
- Create a trait for K8sClient
- Move tables to app schema
- Run cargo fmt
- Update sqlx metadata
- Add images api tests
- Add images api
- Allow configuring of replicator images
- Add api to stop a pipeline
- Use correct prefix
- Wire up start_pipeline endpoint
- Cleanup start_pipeline method
- Force correct nullability in query! macro
- Add api to start a pipeline
- Add update api for publications
- Add create, read and delete api for publications
- Add api to read tables names from source db
- Move name into its own column in publications table
- Show proper error messages in cli
- Return json error messages
- Avoid cross tenant object creation
- Remove supabase_project_ref and prefix from tenant
- Add publication id to pipeline
- Add publication name
- Remove publication field from source
- Update cli help and prompts
- Add publication support to cli
- Associate a publication with a source
- Update sqlx metadata
- Add publication to the api
- Remove toolchain file and use git dep for sqlx
- Use default profile instead of minimal
- Execute reamining cli commands
- Use rust-toolchain with version 1.80
- Remove RUSTSEC-2024-0363 from deny.toml
- Bump sqlx to 0.8.1
- Revert "try to build docker images without a cache to fix build errors"

This reverts commit 726b407c4e36655e9733748003b61e079f68ee0b.
- Try to build docker images without a cache to fix build errors
- Update sqlx metadata
- Read sources, sinks and pipelines in cli
- Create sources, sinks and pipelines in cli
- List tenants in cli
- Delete tenant in cli
- Update tenant in cli
- Read tenant in cli
- Create tenant in cli
- Add apis to read all tenants, sources, sinks and pipelines
- Add a cli to exercise the api
- Add delete pipeline api
- Add update pipeline api
- Add read pipeline api
- Add create pipeline api
- Use a specific revision for tokio-postgres deps
- Add sinks api
- Add delete source api
- Add update source api
- Pass tenant_id via header in create source api
- Update sqlx metadata
- Add read source api
- Factor out a method from a test
- Correct a test case name
- Add create source api
- Add supabase_project_ref and prefix columns to tenants table
- Set RUST_LOG=info if missing
- Disable code coverage for api crate
- Add postgres service to code coverage step
- Update sqlx metadata
- Add delete tenant api
- Add update tenant api
- Add code to manually cleanup test database
- Use separate dbs for each test for test isolation
- Add another test for get tenant api
- Update sqlx metadata
- Add get tenant api
- Add post tenant api
- Move health_check into its own module
- Temporarily quieten failing security advisory check
- Disable rust cache to try to fix failing tests due to low disk space on GH runners
- Update README.md on how to use features
- Add stdout feature for consistency
- Add empty default features and enable all features in rust analyzer
- Merge pull request #26 from funlennysub/sink-features

Put sinks behind feature flags
- Require features for examples
- Put sinks behind feature flags
- Remove config_type from README
- Remove config_type crate
- Remove replicator_image from config
- Do not sleep if task succeeds
- Remove replicator_image from config
- Correct api docker step
- Dump gh context for debugging
- Try both head_ref and ref_name
- Use correct var from context
- Revert "another attempt at using git branch and sha"

This reverts commit 0597eb086c71a9367542afc77c53ccf0e34b462e.
- Another attempt at using git branch and sha
- Use commit sha in docker image tags
- Replicator pod uses postgres secret
- Make worker tasks more granular
- Build replicator image
- Use file instead of context
- Use vars prefix instead of secrets
- Use a variable instead of hardcoded value
- Revert "correct variable format"

This reverts commit 591927b974216855e87185ef55a8d911b5141f80.
- Correct variable format
- Try GIHUB_WORKSPACE variable
- Yet another format
- Use another format for context folder
- Use correct context for docker build
- Build and publish docker image for api
- Make replicator image configurable
- Merge pull request #24 from supabase/abc3-patch-1

change the imor repo to supabase in README.md
- Change the imor repo to supabase in README.md
- Install default crypto provider in replicator
- Enable env-filter feature for replicator
- Better tracing initialization for replicator
- Use a published version of gcp-bigquery-client
- Enable env-filter feature to fix failing tests
- Better tracing initialization
- Move poll_interval_secs to base.yaml config file
- Add Dockerfile for api crate
- Add --motherduck-db-name option to allow the user to pass db name
- Add a note about duckdb performance in README
- Call out motherduck support in README
- Bump duckdb version to 1.0
- Add support for motherduck
- Tweaked README
- Delete existing pod when deleting stateful sets
- Delete existing pods to let the patched stateful set restart another one automatically
- Use stateful sets instead of pods
- Add project ref as prefix to k8s object names
- Add a line in README
- Add a quickstart section to README
- Worker handles delete request
- Rename a few methods
- Avoid creating kube.rs k8s client for every request
- Use patch instead of create for upsert semantics while creating k8s objects
- Worker creates k8s objects
- Factor config-types into a separate crate for reuse
- Improve logging and make worker poll interval configurable
- Add k8s-openapi dependency
- Remove queue project
- Add kube dependency
- Use sqlx macros to verify queries at compile time
- Run api and non-api tests separately
- Run all tests in the workspace
- Disable failing cargo-deny advisory and enable the scheduled tests again
- Stop sqlx from searching for a .pgpass file
- Remove unused dependencies
- Add logging
- Temporarily disabling scheduled security audit as it keeps on failing
- Run tests in CI
- Background works runs
- Connect lazily to db and require ssl if enabled
- Integrate sqlx with actix
- Allow api to be configured
- Tests start api on random port to avoid conflicts
- Add healthcheck test
- Add healthcheck
- Api scaffolding
- Siplify queueing impl
- Update README.md
- Add Apache license
- Use generate always as ... syntax
- Bump prost version to 0.13.1 and fix compiler errors due to that
- Add CI workflow files
- Add empty api crate to workspace
- Dequeue multiple tasks
- Update dequeued task to 'in_progress' before returning from dequeue method
- Add a method to dequeue from the task queue
- Use bigserial instead of serial in queue primary key
- Add a simple queue lib crate
- Reduce docker image size from 1.11 GB to 141 MB
- Remove unnecessary log line
- Ignore .env file from git
- Add a dockerfile for replicator
- Set secrets via env vars
- Replicator now starts the pipeline
- Add replicator binary project
- Use correct branch for gcp-bigquery-client crate
- Use bytes as a fallback type for unsupported types
- Measure & print time to copy tables
- Add missing feature in gcp_bigquery_client dep
- Parse unknown types
- Better error message
- Read null values for nullable columns
- Expose batch config params to cli
- Bump bq batch size to 10,000
- Perf fix: do not write cdc events one at a time
- Add support for text type in bigquery client
- Change log level of a few log lines to debug
- Not truncating table

this is done to workaround the bug in which the default stream is not available
immediately when first creating a table. In that case the table used to be
always created fresh and the stream was not available after each recreation. Not
truncating is fine because we are using BQ cdc enabled tables in which inserting
duplicate UPSERT entries should be fine.
- Replace query impl with more robust version
- Ensure a query job is complete before progressing
- Create tables only once during first run
- Improve logging
- Remove a hack of waiting for the stream to be created

The real fix was to change the stream name format in gcp-bigquery-client repo
from
projects/{projectId}/datasets/{datasetId}/tables/{tableName}/streams/_default to
projects/{projectId}/datasets/{datasetId}/tables/{tableName}/_default
- Improve logging
- Commented out local patch gcp-bigquery-client to help testing
- Add fuse behaviour in BatchTimeoutStream
- Move a code block inside if statement
- Sending status message
- Add a todo
- Improve logging
- Add some missing impls
- Remove dead code and reduce cloning
- Do not use cdc during initial table copy
- Do not suppress response errors
- Increase batch size to 1000
- Remove Sink impl for big query
- Use BatchSink impl
- Table copy working with new append_rows api
- Impl Message for TableRow
- Initial table copy using table.insertAll api
- Add a new batch data copy
- Add a stream which batches underlyig stream's items
- Impl write_table_row and write_cdc_event for BigQuerySink
- Impl table_copied and truncate_table for BigQuerySink
- Impl write_table_schemas for BigQuerySink
- Impl get_resumption_state for BigQuerySink
- Use arrays instead of Vecs
- Separate DuckDb sink and executor in separate modules
- Improve an error message
- Use pure ascii chars in diagrams and prevent formatting in markdown
- Add ascii diagrams
- Add support for bool, bytea and bpchar types in duckdb sink
- Add data copy and performance senctions to README
- Update README.md
- Send status update
- Remove unnecessary methods in Sink trait
- Cdc resumption working
- Cdc resumption wip
- Truncate partially copied tables
- Table copy state saved and resumed
- Return errors from DuckDbExecutor
- Table copy state wip 1
- Table copy state wip
- Handle delete cdc event
- Handle update cdc event
- Handle insert cdc event
- Remove json converters
- Table copy to duckdb working
- Remove explicit close
- Wip
- Create duckdb schema and tables
- Copy table schemas to sink
- Use CdcEvent instead of generic converters
- Use TableRow instead of generic converters
- Wip
- Remove and unnecessary transaction commit
- Remove some duplicate code
- Cdc stream also through pipeline
- Copy tables stdout sink
- Conversion to TableRow structs
- Add sink trait
- Module changes
- Port changes from the experimental repo
- Ignoring table with unsupported types instead of stopping replication
- Remove some unwrap calls
- Remove a panic call
- Remove an expect call
- Remove some expect calls
- Remove some expect calls
- Convert expect calls into errors
- Remove calls to unnecessary unwraps
- Add support for json and jsonb types
- Use ciborium instead of serde_cbor
- Add support for more integer types
- Add support for more text types
- Add support for char type
- Add support for bpchar type
- Add support for bool and bytea types
- Remove commented out code
- Save partially filled buffer if buffer fill timeout expires
- Remove hardcoded events per file
- Minor cleanup
- Copy tables and realtime changes
- Read resumption data
- Add resumption logic
- Client can pass resume lsn
- Add replicate-to-s3
- Make a workspace
- Create permanent slot
- Collect events in memory before writing to parquet file
- Emit identity and type_modifier cols in schema
- Emit replication events in realtime changes
- Snapshot example
- Use serde to serialize events
- Remove unused import
- Use chrono for date/time
- Print in json format
- Copy table schema within transaction
- Add missing query in docs
- Add description in README
- Add stdout example
- Add some docs
- Move non-essential dependencies to dev-dependencies
- Add support for varchar and timestamp
- Rename example
- Cleanup
- Better api
- Write test parquet file
- Initial commit

### Refactors

- *(ci)* Open a PR before releasing a new version (#318)
- *(tests)* Fix flaky integration test (#314)
- *(tests)* Rework integration tests structure (#313)
- *(docs)* Add `CleanupStore` to docs (#302)
- *(sql)* Make queries lowercase (#298)
- *(bigquery)* Remove fallback handling in BigQuery (#293)
-  ref(types): Rework how types are structured and exported (#290)
- *(migrations)* Reset migrations to start from a clean state (#289)
- *(k8s)* Improve update behavior of pods in the replicator statefulset (#284)
- *(api)* Rework how configs are dealt with in the API (#282)
- *(etl)* Move mappings from schema store to state store and remove pipeline id (#264)
- *(errors)* Default errors to be manually retriable and improve error kinds (#258)
- *(api)* Improve OpenAPI docs (#248)
- *(store)* Put NotifyStateStore into test_utils (#238)
- *(api)* Do not return detailed pod error stats (#237)
- *(readme)* Remove outdated section in README (#236)
- *(crates)* Rename crates with  prefix `etl` prefix (#234)
- *(schema)* Move schema into the schema store and remove schema cache (#231)
- *(panics)* Abort process on any panic (#226)
- *(dependencies)* Remove usage of forks for tokio dependencies and vendor tokio-postgres-rustls (#225)
- *(core)* Rework how errors are handled in table sync workers (#217)
- *(modules)* Rework module exports and split destinations into their own crate (#224)
- *(deps)* Update all dependencies and removed unused ones (#220)
- *(tests)* Remove BigQuery emulator (#211)
- *(logs)* Improve logs phrashing (#208)
- *(locking)* Use Mutex instead of RwLock in certain places (#202)
- *(db)* Remove publication_name from db (#183)
- *(logs)* Improve logs (#181)
- *(ci)* Simplify output (#177)
- Remove duplicate config between the `etl` and the `config` crates (#161)
- Remove duplicate Postgres connection config structs (#160)
- Move trusted root certs into tls config (#157)
- *(api)* Improve utopia docs and API (#153)
- *(postgres)* Rename `PgDatabaseOptions` structs (#145)
- *(docs)* Improve READMEs (#137)
- Factor out repeated hardcoded strings into constants
- *(tests)* Improve integration tests
- Reference publications by name
