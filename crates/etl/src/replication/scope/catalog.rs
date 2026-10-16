//! Bounded physical-source discovery. Destination schemas and admission rules
//! stay outside the engine; callers receive typed PostgreSQL facts.
use anyhow::Context;

use super::{Column, Connection, Relation, Row};

pub struct Identity {
    pub system_id: String,
    pub database_oid: String,
    pub server_version: u32,
}
pub struct CatalogColumn {
    pub column: Column,
    pub not_null: bool,
    pub generated: bool,
    pub attribute_number: u16,
    pub collation_oid: u32,
}
pub struct Table {
    pub relation: Relation,
    pub columns: Vec<CatalogColumn>,
    pub ordinary: bool,
    pub row_security: bool,
    pub partition: bool,
    pub column_filter: bool,
    pub row_filter: bool,
}
pub struct Catalog {
    pub identity: Identity,
    pub primary: bool,
    pub logical_wal: bool,
    pub slot_wal_limit: i64,
    pub complete_publication: bool,
    pub source_helpers: bool,
    pub tables: Vec<Table>,
}

pub struct SlotState {
    pub confirmed: u64,
    pub is_available: bool,
}

pub async fn slot_state(sql: &mut Connection, slot: &str) -> anyhow::Result<Option<SlotState>> {
    super::identifier(slot)?;
    let rows = sql
        .query(&format!(
            "SELECT plugin,database,confirmed_flush_lsn::text,wal_status FROM \
             pg_replication_slots WHERE slot_name='{slot}' AND database=current_database() AND \
             slot_type='logical'"
        ))
        .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    anyhow::ensure!(
        rows.len() == 1 && field(row, 0)? == "pgoutput",
        "replication slot identity differs"
    );
    Ok(Some(SlotState {
        confirmed: super::lsn(field(row, 2)?)?,
        is_available: matches!(field(row, 3)?, "reserved" | "extended"),
    }))
}

/// The host first verifies its durable ownership and retirement intent.
/// Missing slots are already retired; foreign plugins are never dropped.
pub async fn drop_slot(sql: &mut Connection, slot: &str) -> anyhow::Result<()> {
    if slot_state(sql, slot).await?.is_some() {
        sql.query(&format!("SELECT pg_drop_replication_slot('{slot}')")).await?;
    }
    anyhow::ensure!(slot_state(sql, slot).await?.is_none(), "replication slot cleanup incomplete");
    Ok(())
}

/// Hold source lifecycle ownership on the backend that creates the slot and
/// enters CopyBoth. Never leave a blocking server-side lock waiter after
/// cancellation. The host supplies its lock namespace and handoff deadline.
pub async fn lock_source(
    connection: &mut Connection,
    key: i64,
    deadline: std::time::Duration,
) -> anyhow::Result<()> {
    tokio::time::timeout(deadline, async {
        loop {
            let rows =
                connection.query(&format!("SELECT pg_try_advisory_lock({key})::text")).await?;
            match field(rows.first().context("source lock response absent")?, 0)? {
                "true" => return Ok::<(), anyhow::Error>(()),
                "false" => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
                _ => anyhow::bail!("source lock response differs"),
            }
        }
    })
    .await?
}

fn field(row: &Row, index: usize) -> anyhow::Result<&str> {
    row.get(index)
        .context("PostgreSQL catalog column missing")?
        .as_deref()
        .context("PostgreSQL catalog column is NULL")
}
fn unsigned(value: &str) -> anyhow::Result<u32> {
    let parsed: u32 = value.parse()?;
    anyhow::ensure!(parsed.to_string() == value, "noncanonical PostgreSQL catalog integer");
    Ok(parsed)
}
fn signed(value: &str) -> anyhow::Result<i32> {
    let parsed: i32 = value.parse()?;
    anyhow::ensure!(parsed.to_string() == value, "noncanonical PostgreSQL catalog integer");
    Ok(parsed)
}
fn boolean(value: &str) -> anyhow::Result<bool> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => anyhow::bail!("invalid PostgreSQL catalog boolean"),
    }
}

pub async fn identity(connection: &mut Connection) -> anyhow::Result<Identity> {
    let rows = connection
        .query(
            "SELECT system_identifier::text,(SELECT oid::text FROM pg_database WHERE \
             datname=current_database()),current_setting('server_version_num') FROM \
             pg_control_system()",
        )
        .await?;
    let row = rows.first().context("PostgreSQL source identity missing")?;
    let system = field(row, 0)?;
    let parsed = system.parse::<u64>()?;
    anyhow::ensure!(parsed.to_string() == system, "noncanonical PostgreSQL system identity");
    Ok(Identity {
        system_id: system.to_owned(),
        database_oid: unsigned(field(row, 1)?)?.to_string(),
        server_version: unsigned(field(row, 2)?)?,
    })
}

pub async fn primary_timeline(connection: &mut Connection) -> anyhow::Result<u32> {
    let role = connection.query("SELECT pg_is_in_recovery()::text").await?;
    anyhow::ensure!(
        field(role.first().context("PostgreSQL source role missing")?, 0)? == "false",
        super::CaptureRefusal::ContinuityLost
    );
    let rows = connection.query("IDENTIFY_SYSTEM").await?;
    let timeline = unsigned(field(rows.first().context("PostgreSQL timeline missing")?, 1)?)?;
    anyhow::ensure!(timeline > 0, super::CaptureRefusal::ContinuityLost);
    Ok(timeline)
}

pub async fn read(
    connection: &mut Connection,
    publication: &str,
    maximum_tables: usize,
    maximum_columns: usize,
) -> anyhow::Result<Catalog> {
    super::identifier(publication)?;
    let identity = identity(connection).await?;
    let settings = connection
        .query(
            "SELECT current_setting('wal_level'),pg_is_in_recovery()::text,\
             pg_size_bytes(current_setting('max_slot_wal_keep_size'))::text",
        )
        .await?;
    let settings = settings.first().context("PostgreSQL replication settings missing")?;
    let slot_wal_limit = field(settings, 2)?.parse::<i64>()?;
    let logical_wal = field(settings, 0)? == "logical";
    let primary = !boolean(field(settings, 1)?)?;
    let helpers = connection
        .query(
            "SELECT EXISTS (SELECT 1 FROM pg_event_trigger t JOIN pg_proc f ON f.oid=t.evtfoid \
             JOIN pg_namespace n ON n.oid=f.pronamespace WHERE \
             t.evtname='supabase_etl_ddl_message_trigger' AND t.evtevent='ddl_command_end' AND \
             t.evtenabled IN ('O','A') AND t.evttags @> ARRAY['ALTER TABLE','ALTER \
             PUBLICATION']::text[] AND n.nspname='etl' AND \
             f.proname='emit_schema_change_messages')::text,has_function_privilege(current_user,'\
             pg_catalog.pg_logical_emit_message(boolean,text,text)','EXECUTE')::text",
        )
        .await?;
    let helpers = helpers.first().context("PostgreSQL source helper preflight missing")?;
    let source_helpers = boolean(field(helpers, 0)?)? && boolean(field(helpers, 1)?)?;
    let flags = connection
        .query(&format!(
            "SELECT puballtables::text,pubinsert::text,pubupdate::text,pubdelete::text,\
             pubtruncate::text,pubviaroot::text FROM pg_publication WHERE pubname='{publication}'"
        ))
        .await?;
    anyhow::ensure!(
        flags.len() == 1 && flags[0].len() == 6,
        "PostgreSQL publication missing or ambiguous"
    );
    let flags = flags[0]
        .iter()
        .map(|value| boolean(value.as_deref().context("publication flag is NULL")?))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let complete_publication = !flags[0] && flags[1..5].iter().all(|flag| *flag) && !flags[5];
    let published = connection
        .query(&format!(
            "SELECT n.nspname,c.relname,c.oid::text,c.relkind::text,c.relreplident::text,c.\
             relrowsecurity::text,c.relforcerowsecurity::text,c.relispartition::text,(pr.prattrs \
             IS NULL)::text,(pr.prqual IS NULL)::text FROM pg_publication_rel pr JOIN \
             pg_publication p ON p.oid=pr.prpubid JOIN pg_class c ON c.oid=pr.prrelid JOIN \
             pg_namespace n ON n.oid=c.relnamespace WHERE p.pubname='{publication}' ORDER BY \
             n.nspname,c.relname"
        ))
        .await?;
    anyhow::ensure!(
        !published.is_empty() && published.len() <= maximum_tables,
        "PostgreSQL publication exceeds source scope bound"
    );
    let mut tables = Vec::new();
    for row in published {
        let oid = unsigned(field(&row, 2)?)?;
        let raw_columns = connection
            .query(&format!(
                "SELECT a.attname,a.atttypid::text,a.atttypmod::text,EXISTS(SELECT 1 FROM \
                 pg_index i WHERE i.indrelid=a.attrelid AND i.indisprimary AND i.indisvalid AND \
                 i.indimmediate AND \
                 a.attnum=ANY(i.indkey))::text,a.attnotnull::text,a.attgenerated::text,a.\
                 attnum::text,a.attcollation::text FROM pg_attribute a WHERE a.attrelid={oid} AND \
                 a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum"
            ))
            .await?;
        anyhow::ensure!(
            !raw_columns.is_empty() && raw_columns.len() <= maximum_columns,
            "PostgreSQL relation exceeds column bound"
        );
        let columns = raw_columns
            .into_iter()
            .map(|column| {
                Ok(CatalogColumn {
                    column: Column {
                        name: field(&column, 0)?.to_owned(),
                        type_oid: unsigned(field(&column, 1)?)?,
                        type_modifier: signed(field(&column, 2)?)?,
                        key: boolean(field(&column, 3)?)?,
                    },
                    not_null: boolean(field(&column, 4)?)?,
                    generated: !field(&column, 5)?.is_empty(),
                    attribute_number: u16::try_from(unsigned(field(&column, 6)?)?)?,
                    collation_oid: unsigned(field(&column, 7)?)?,
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        let replica_identity = field(&row, 4)?.as_bytes();
        anyhow::ensure!(replica_identity.len() == 1, "invalid PostgreSQL replica identity");
        tables.push(Table {
            relation: Relation {
                oid,
                namespace: field(&row, 0)?.to_owned(),
                name: field(&row, 1)?.to_owned(),
                replica_identity: replica_identity[0],
                columns: columns.iter().map(|column| column.column.clone()).collect(),
            },
            columns,
            ordinary: field(&row, 3)? == "r",
            row_security: boolean(field(&row, 5)?)? || boolean(field(&row, 6)?)?,
            partition: boolean(field(&row, 7)?)?,
            column_filter: !boolean(field(&row, 8)?)?,
            row_filter: !boolean(field(&row, 9)?)?,
        });
    }
    Ok(Catalog {
        identity,
        primary,
        logical_wal,
        slot_wal_limit,
        complete_publication,
        source_helpers,
        tables,
    })
}
