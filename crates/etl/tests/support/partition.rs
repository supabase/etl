use etl::types::TableId;
use etl_postgres::{tokio::test_utils::PgDatabase, types::TableName};
use tokio_postgres::Client;

#[derive(Debug)]
pub(crate) struct NestedPartitionHierarchy {
    pub(crate) root_table_name: TableName,
    pub(crate) p_2026_table_name: TableName,
    pub(crate) root_table_id: TableId,
    pub(crate) p_2025_01_table_id: TableId,
    pub(crate) p_2025_02_table_id: TableId,
    pub(crate) p_2026_table_id: TableId,
    pub(crate) p_2026_01_table_id: TableId,
    pub(crate) p_2026_02_table_id: TableId,
}

pub(crate) fn partition_table_name(table_name: &TableName, partition_name: &str) -> TableName {
    TableName::new(table_name.schema.clone(), format!("{}_{}", table_name.name, partition_name))
}

pub(crate) async fn create_nested_partition_hierarchy(
    database: &PgDatabase<Client>,
    table_name: TableName,
) -> NestedPartitionHierarchy {
    database
        .run_sql(&format!(
            "create table {} (
                id bigserial,
                data text not null,
                partition_year integer not null,
                partition_month integer not null,
                primary key (id, partition_year, partition_month)
            ) partition by range (partition_year)",
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let p_2025_table_name =
        TableName::new(table_name.schema.clone(), format!("{}_2025", table_name.name));
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (2025) to (2026)
             partition by range (partition_month)",
            p_2025_table_name.as_quoted_identifier(),
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let p_2025_01_table_name = partition_table_name(&p_2025_table_name, "01");
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (1) to (2)",
            p_2025_01_table_name.as_quoted_identifier(),
            p_2025_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let p_2025_02_table_name = partition_table_name(&p_2025_table_name, "02");
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (2) to (3)",
            p_2025_02_table_name.as_quoted_identifier(),
            p_2025_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let p_2026_table_name =
        TableName::new(table_name.schema.clone(), format!("{}_2026", table_name.name));
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (2026) to (2027)
             partition by range (partition_month)",
            p_2026_table_name.as_quoted_identifier(),
            table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let p_2026_01_table_name = partition_table_name(&p_2026_table_name, "01");
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (1) to (2)",
            p_2026_01_table_name.as_quoted_identifier(),
            p_2026_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    let p_2026_02_table_name = partition_table_name(&p_2026_table_name, "02");
    database
        .run_sql(&format!(
            "create table {} partition of {} for values from (2) to (3)",
            p_2026_02_table_name.as_quoted_identifier(),
            p_2026_table_name.as_quoted_identifier()
        ))
        .await
        .unwrap();

    NestedPartitionHierarchy {
        root_table_id: get_table_id(database, &table_name).await,
        p_2025_01_table_id: get_table_id(database, &p_2025_01_table_name).await,
        p_2025_02_table_id: get_table_id(database, &p_2025_02_table_name).await,
        p_2026_table_id: get_table_id(database, &p_2026_table_name).await,
        p_2026_01_table_id: get_table_id(database, &p_2026_01_table_name).await,
        p_2026_02_table_id: get_table_id(database, &p_2026_02_table_name).await,
        root_table_name: table_name,
        p_2026_table_name,
    }
}

async fn get_table_id(database: &PgDatabase<Client>, table_name: &TableName) -> TableId {
    let row = database
        .client
        .as_ref()
        .unwrap()
        .query_one(
            "select c.oid
             from pg_class c
             join pg_namespace n on n.oid = c.relnamespace
             where n.nspname = $1 and c.relname = $2",
            &[&table_name.schema, &table_name.name],
        )
        .await
        .unwrap();

    row.get(0)
}
