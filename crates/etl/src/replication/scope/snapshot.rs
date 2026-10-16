//! One exported snapshot shared by all copy readers. Copy-attempt state is
//! private until the destination seals the complete scope manifest.
use anyhow::Context;
use serde::{Deserialize, Serialize};

use super::{Connection, QueryRows, Relation, Row};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Partition {
    pub relation: u32,
    pub selection: Selection,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Selection {
    All,
    IntegerRange { lower: Option<i64>, upper: Option<i64> },
}

fn quote(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn names(relation: &Relation, keys: bool) -> String {
    relation
        .columns
        .iter()
        .filter(|column| !keys || column.key)
        .map(|column| quote(&column.name))
        .collect::<Vec<_>>()
        .join(",")
}

fn predicate(relation: &Relation, selection: &Selection) -> anyhow::Result<String> {
    match selection {
        Selection::All => Ok(String::new()),
        Selection::IntegerRange { lower, upper } => {
            let keys: Vec<_> = relation.columns.iter().filter(|column| column.key).collect();
            anyhow::ensure!(
                keys.len() == 1 && matches!(keys[0].type_oid, 20 | 21 | 23),
                "snapshot range requires one integer key"
            );
            anyhow::ensure!(
                lower.zip(*upper).is_none_or(|(lower, upper)| lower < upper),
                "empty snapshot range"
            );
            let mut terms = Vec::new();
            if let Some(lower) = lower {
                terms.push(format!("{} >= {lower}", quote(&keys[0].name)));
            }
            if let Some(upper) = upper {
                terms.push(format!("{} < {upper}", quote(&keys[0].name)));
            }
            Ok(if terms.is_empty() {
                String::new()
            } else {
                format!("WHERE {}", terms.join(" AND "))
            })
        }
    }
}

/// The caller first durably records ownership of `slot`. All readers must be
/// connected before export; no command may invalidate the exported snapshot
/// until every reader imports it. Any failed import abandons the whole cohort.
pub async fn begin(
    exporter: &mut Connection,
    guard: &mut Connection,
    readers: &mut [&mut Connection],
    slot: &str,
    relations: &[Relation],
) -> anyhow::Result<u64> {
    super::identifier(slot)?;
    anyhow::ensure!(
        !readers.is_empty() && !relations.is_empty(),
        "snapshot scope and readers must be nonempty"
    );
    let scope: std::collections::BTreeSet<_> =
        relations.iter().map(|relation| relation.oid).collect();
    anyhow::ensure!(scope.len() == relations.len(), "snapshot scope contains duplicate relations");
    let tables = relations
        .iter()
        .map(|relation| format!("{}.{}", quote(&relation.namespace), quote(&relation.name)))
        .collect::<Vec<_>>()
        .join(",");
    // Protect layout before establishing the cut: a table rewrite between
    // export and the first import lock can otherwise invalidate old tuples.
    // The imported readers acquire their own locks before this guard releases.
    guard
        .query(&format!("BEGIN READ ONLY; LOCK TABLE {tables} IN ACCESS SHARE MODE NOWAIT"))
        .await?;
    let result = exporter
        .query(&format!("CREATE_REPLICATION_SLOT {slot} LOGICAL pgoutput (SNAPSHOT 'export')"))
        .await?;
    let result = result.first().context("snapshot slot creation returned no boundary")?;
    anyhow::ensure!(
        result.len() == 4
            && result[0].as_deref() == Some(slot)
            && result[3].as_deref() == Some("pgoutput"),
        "created slot differs from the owned source intent"
    );
    let point =
        super::lsn(result.get(1).and_then(Option::as_deref).context("snapshot position missing")?)?;
    anyhow::ensure!(point > 0, "exported snapshot has no source boundary");
    let snapshot = result.get(2).and_then(Option::as_deref).context("exported snapshot missing")?;
    anyhow::ensure!(
        !snapshot.is_empty()
            && snapshot.len() <= 128
            && snapshot.bytes().all(|byte| byte.is_ascii_hexdigit() || byte == b'-'),
        "invalid exported snapshot identity"
    );
    for reader in readers.iter_mut() {
        reader
            .query(&format!(
                "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT \
                 '{snapshot}'; SET LOCAL timezone='UTC'; SET LOCAL datestyle='ISO, YMD'; SET \
                 LOCAL bytea_output='hex'; SET LOCAL extra_float_digits=3; SET LOCAL \
                 intervalstyle='iso_8601'"
            ))
            .await?;
    }
    for reader in readers.iter_mut() {
        // Every reader keeps the whole layout protected, even if another
        // reader finishes first. Queued DDL abandons this attempt rather than
        // waiting behind a lock that this same cohort already holds.
        reader.query(&format!("LOCK TABLE {tables} IN ACCESS SHARE MODE NOWAIT")).await?;
    }
    for reader in readers.iter_mut() {
        reader.snapshot = Some((
            snapshot.to_owned(),
            point,
            relations.iter().map(|relation| relation.oid).collect(),
        ));
    }
    guard.query("COMMIT").await?;
    Ok(point)
}

/// Disjoint half-open partitions over the full signed int8 domain.
pub fn integer_ranges(
    minimum: i64,
    maximum: i64,
    partitions: usize,
) -> anyhow::Result<Vec<Selection>> {
    anyhow::ensure!(
        minimum <= maximum && (1..=64).contains(&partitions),
        "invalid snapshot extrema or partition bound"
    );
    let width = i128::from(maximum) - i128::from(minimum) + 1;
    let count = width.min(partitions as i128);
    let mut selections = Vec::new();
    for index in 0..count {
        let boundary = |part| i64::try_from(i128::from(minimum) + width * part / count);
        selections.push(Selection::IntegerRange {
            lower: (index > 0).then(|| boundary(index)).transpose()?,
            upper: (index + 1 < count).then(|| boundary(index + 1)).transpose()?,
        });
    }
    Ok(selections)
}

/// Partitioning is deterministic over source keys and independent of worker
/// scheduling. The entire int8 domain is calculated in i128 without overflow.
pub async fn freeze(
    connection: &mut Connection,
    relations: &[Relation],
    partitions: usize,
) -> anyhow::Result<Vec<Partition>> {
    anyhow::ensure!(partitions > 0 && partitions <= 64, "invalid snapshot partition bound");
    let mut relations: Vec<_> = relations.iter().collect();
    relations.sort_by_key(|relation| relation.oid);
    let mut result = Vec::new();
    for relation in relations {
        let keys: Vec<_> = relation.columns.iter().filter(|column| column.key).collect();
        let mut selections = vec![Selection::All];
        if keys.len() == 1 && matches!(keys[0].type_oid, 20 | 21 | 23) {
            let extrema = connection
                .query(&format!(
                    "SELECT MIN({})::text,MAX({})::text FROM {}.{}",
                    quote(&keys[0].name),
                    quote(&keys[0].name),
                    quote(&relation.namespace),
                    quote(&relation.name)
                ))
                .await?;
            anyhow::ensure!(
                extrema.len() == 1 && extrema[0].len() == 2,
                "snapshot extrema are incomplete"
            );
            match (&extrema[0][0], &extrema[0][1]) {
                (None, None) => {}
                (Some(minimum), Some(maximum)) => {
                    let minimum = minimum.parse::<i64>()?;
                    let maximum = maximum.parse::<i64>()?;
                    selections = integer_ranges(minimum, maximum, partitions)?;
                }
                _ => anyhow::bail!("snapshot extrema disagree"),
            }
        }
        result.extend(
            selections.into_iter().map(|selection| Partition { relation: relation.oid, selection }),
        );
    }
    Ok(result)
}

/// A bounded copy reader on an imported cohort snapshot. It streams simple
/// query rows rather than collecting a table or a whole FETCH response.
pub struct Cursor {
    connection: Connection,
    fetch: Option<QueryRows>,
    open: bool,
    exhausted: bool,
    fetch_rows: usize,
    active: Option<Partition>,
    complete: Vec<Partition>,
    finished: bool,
}

/// Only an exhausted reader on an imported snapshot can produce this seal.
#[derive(Clone)]
pub struct CopySeal {
    snapshot: String,
    position: u64,
    complete: Vec<Partition>,
    scope: Vec<u32>,
}

/// Verify a complete source cut, including every empty partition. Ready flags
/// from independently initialized tables cannot construct this evidence.
pub fn seal_scope(plan: &[Partition], seals: &[CopySeal]) -> anyhow::Result<u64> {
    let first = seals.first().context("snapshot has no reader seals")?;
    anyhow::ensure!(
        !first.snapshot.is_empty() && !plan.is_empty(),
        "snapshot completion has no scope"
    );
    let expected_relations =
        plan.iter().map(|partition| partition.relation).collect::<std::collections::BTreeSet<_>>();
    anyhow::ensure!(
        expected_relations == first.scope.iter().copied().collect(),
        "snapshot plan omits part of its source scope"
    );
    let mut completed = Vec::new();
    for seal in seals {
        anyhow::ensure!(
            seal.snapshot == first.snapshot
                && seal.position == first.position
                && seal.scope == first.scope,
            "snapshot readers have different source cuts"
        );
        completed.extend(seal.complete.iter().cloned());
    }
    // A canonical serialization provides equality without imposing a generic
    // ordering on provider keys. Duplicate or missing selections both refuse.
    let mut expected = plan.iter().map(serde_json::to_string).collect::<Result<Vec<_>, _>>()?;
    let mut actual = completed.iter().map(serde_json::to_string).collect::<Result<Vec<_>, _>>()?;
    expected.sort();
    actual.sort();
    anyhow::ensure!(
        expected == actual && expected.windows(2).all(|pair| pair[0] != pair[1]),
        "snapshot scope is incomplete or duplicated"
    );
    Ok(first.position)
}

impl Cursor {
    pub fn new(connection: Connection, fetch_rows: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            connection.snapshot.is_some() && fetch_rows > 0,
            "copy reader has no imported source snapshot"
        );
        Ok(Self {
            connection,
            fetch: None,
            open: false,
            exhausted: false,
            fetch_rows,
            active: None,
            complete: Vec::new(),
            finished: false,
        })
    }

    pub async fn select(
        &mut self,
        relation: &Relation,
        selection: &Selection,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.open && self.fetch.is_none() && !self.finished,
            "snapshot selection is already open or sealed"
        );
        self.connection
            .query(&format!(
                "DECLARE etl_snapshot NO SCROLL CURSOR FOR SELECT {} FROM {}.{} {} ORDER BY {}",
                names(relation, false),
                quote(&relation.namespace),
                quote(&relation.name),
                predicate(relation, selection)?,
                names(relation, true)
            ))
            .await?;
        self.open = true;
        self.exhausted = false;
        self.active = Some(Partition { relation: relation.oid, selection: selection.clone() });
        Ok(())
    }

    pub async fn next(&mut self) -> anyhow::Result<Option<Row>> {
        anyhow::ensure!(self.open, "snapshot selection is not open");
        loop {
            if let Some(fetch) = &mut self.fetch {
                if let Some(row) = fetch.next(&mut self.connection).await? {
                    return Ok(Some(row));
                }
                self.exhausted = fetch.rows() < self.fetch_rows;
                self.fetch = None;
            }
            if self.exhausted {
                self.connection.query("CLOSE etl_snapshot").await?;
                self.open = false;
                self.complete
                    .push(self.active.take().context("snapshot selection identity missing")?);
                return Ok(None);
            }
            self.fetch = Some(
                self.connection
                    .query_rows(
                        &format!("FETCH FORWARD {} FROM etl_snapshot", self.fetch_rows),
                        self.fetch_rows,
                    )
                    .await?,
            );
        }
    }

    pub async fn finish(&mut self) -> anyhow::Result<CopySeal> {
        anyhow::ensure!(
            !self.open && self.fetch.is_none(),
            "snapshot reader has unfinished selections"
        );
        anyhow::ensure!(!self.finished, "copy reader was already sealed");
        self.connection.query("COMMIT").await?;
        self.finished = true;
        let (snapshot, position, scope) =
            self.connection.snapshot.as_ref().context("copy reader snapshot missing")?;
        Ok(CopySeal {
            snapshot: snapshot.clone(),
            position: *position,
            complete: self.complete.clone(),
            scope: scope.clone(),
        })
    }
}

/// Scan the imported cohort concurrently, with at most one bounded page and
/// one lookahead row per reader. Destination callbacks are serialized, but no
/// source I/O holds their lock. Each partition stays on one reader, so its
/// pages and terminal marker retain source order even when readers interleave.
pub async fn copy<D: crate::pipeline::CaptureDestination>(
    readers: Vec<Cursor>,
    plan: &[Partition],
    relations: &[Relation],
    page_bytes: usize,
    destination: &mut D,
) -> anyhow::Result<Vec<CopySeal>> {
    use super::pipeline::SnapshotRow;

    let width = readers.len();
    anyhow::ensure!(width > 0 && page_bytes > 2, "invalid snapshot copy bounds");
    let destination = tokio::sync::Mutex::new(destination);
    futures::future::try_join_all(readers.into_iter().enumerate().map(|(reader, mut cursor)| {
        let destination = &destination;
        async move {
            for (partition, selection) in plan.iter().enumerate().skip(reader).step_by(width) {
                let relation = relations
                    .iter()
                    .find(|relation| relation.oid == selection.relation)
                    .context("scope relation absent")?;
                destination
                    .lock()
                    .await
                    .authorize()
                    .await
                    .map_err(|error| error.context(super::AuthorizationFailure))?;
                cursor.select(relation, &selection.selection).await?;
                let mut rows = Vec::new();
                // Include JSON array delimiters and a separator for each row.
                let mut bytes = 2usize;
                let mut before_authorize = 0;
                loop {
                    if before_authorize == 0 {
                        destination
                            .lock()
                            .await
                            .authorize()
                            .await
                            .map_err(|error| error.context(super::AuthorizationFailure))?;
                        before_authorize = cursor.fetch_rows;
                    }
                    let Some(values) = cursor.next().await? else {
                        break;
                    };
                    before_authorize -= 1;
                    let row = SnapshotRow {
                        relation: relation.oid,
                        cells: values
                            .into_iter()
                            .map(|value| value.map_or(super::Cell::Null, super::Cell::Text))
                            .collect(),
                    };
                    let size = serde_json::to_vec(&row)?.len() + 1;
                    anyhow::ensure!(size <= page_bytes - 2, super::CaptureRefusal::Capacity);
                    if !rows.is_empty() && bytes + size > page_bytes {
                        let mut destination = destination.lock().await;
                        destination
                            .authorize()
                            .await
                            .map_err(|error| error.context(super::AuthorizationFailure))?;
                        destination
                            .snapshot_page(partition, std::mem::take(&mut rows), false)
                            .await?;
                        bytes = 2;
                    }
                    bytes += size;
                    rows.push(row);
                }
                let mut destination = destination.lock().await;
                destination
                    .authorize()
                    .await
                    .map_err(|error| error.context(super::AuthorizationFailure))?;
                destination.snapshot_page(partition, rows, true).await?;
            }
            cursor.finish().await
        }
    }))
    .await
}
