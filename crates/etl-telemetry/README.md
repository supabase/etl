# `etl-telemetry`

Tracing and Prometheus setup shared by [Supabase ETL](https://supabase.github.io/etl/)
binaries. Most applications configure this from `etl-replicator` or `etl-api`;
library embeddings can initialize tracing themselves, as the
[First Pipeline](https://supabase.github.io/etl/guides/first-pipeline/) tutorial
does with `tracing-subscriber`.