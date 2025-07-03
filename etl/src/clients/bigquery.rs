use std::{collections::HashSet, fs};

use futures::StreamExt;
use gcp_bigquery_client::storage::{ColumnMode, StorageApi};
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::{
    Client,
    error::BQError,
    google::cloud::bigquery::storage::v1::{WriteStream, WriteStreamView},
    model::{
        query_request::QueryRequest, query_response::ResultSet,
        table_data_insert_all_request::TableDataInsertAllRequest,
    },
    storage::{ColumnType, FieldDescriptor, StreamName, TableDescriptor},
};
use postgres::schema::{ColumnSchema, TableId, TableSchema};
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::conversions::Cell;
use crate::conversions::table_row::TableRow;

pub struct BigQueryClient {
    project_id: String,
    client: Client,
}

//TODO: fix all SQL injections
impl BigQueryClient {
    pub async fn new_with_key_path(
        project_id: String,
        gcp_sa_key_path: &str,
    ) -> Result<BigQueryClient, BQError> {
        let gcp_sa_key = fs::read_to_string(gcp_sa_key_path)?;
        let service_account_key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(service_account_key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    pub async fn new_with_key(
        project_id: String,
        gcp_sa_key: &str,
    ) -> Result<BigQueryClient, BQError> {
        let service_account_key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(service_account_key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    pub async fn create_table_if_missing(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: u16,
    ) -> Result<bool, BQError> {
        if self.table_exists(dataset_id, table_name).await? {
            Ok(false)
        } else {
            self.create_table(dataset_id, table_name, column_schemas, max_staleness_mins)
                .await?;
            Ok(true)
        }
    }

    fn postgres_to_bigquery_type(typ: &Type) -> &'static str {
        match typ {
            &Type::BOOL => "bool",
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "string",
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "int64",
            &Type::FLOAT4 | &Type::FLOAT8 => "float64",
            &Type::NUMERIC => "bignumeric",
            &Type::DATE => "date",
            &Type::TIME => "time",
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => "timestamp",
            &Type::UUID => "string",
            &Type::JSON | &Type::JSONB => "json",
            &Type::OID => "int64",
            &Type::BYTEA => "bytes",
            &Type::BOOL_ARRAY => "array<bool>",
            &Type::CHAR_ARRAY
            | &Type::BPCHAR_ARRAY
            | &Type::VARCHAR_ARRAY
            | &Type::NAME_ARRAY
            | &Type::TEXT_ARRAY => "array<string>",
            &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY => "array<int64>",
            &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY => "array<float64>",
            &Type::NUMERIC_ARRAY => "array<bignumeric>",
            &Type::DATE_ARRAY => "array<date>",
            &Type::TIME_ARRAY => "array<time>",
            &Type::TIMESTAMP_ARRAY | &Type::TIMESTAMPTZ_ARRAY => "array<timestamp>",
            &Type::UUID_ARRAY => "array<string>",
            &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "array<json>",
            &Type::OID_ARRAY => "array<int64>",
            &Type::BYTEA_ARRAY => "array<bytes>",
            _ => "string",
        }
    }

    fn is_array_type(typ: &Type) -> bool {
        matches!(
            typ,
            &Type::BOOL_ARRAY
                | &Type::CHAR_ARRAY
                | &Type::BPCHAR_ARRAY
                | &Type::VARCHAR_ARRAY
                | &Type::NAME_ARRAY
                | &Type::TEXT_ARRAY
                | &Type::INT2_ARRAY
                | &Type::INT4_ARRAY
                | &Type::INT8_ARRAY
                | &Type::FLOAT4_ARRAY
                | &Type::FLOAT8_ARRAY
                | &Type::NUMERIC_ARRAY
                | &Type::DATE_ARRAY
                | &Type::TIME_ARRAY
                | &Type::TIMESTAMP_ARRAY
                | &Type::TIMESTAMPTZ_ARRAY
                | &Type::UUID_ARRAY
                | &Type::JSON_ARRAY
                | &Type::JSONB_ARRAY
                | &Type::OID_ARRAY
                | &Type::BYTEA_ARRAY
        )
    }

    fn column_spec(column_schema: &ColumnSchema, s: &mut String) {
        s.push('`');
        s.push_str(&column_schema.name);
        s.push('`');
        s.push(' ');
        let typ = Self::postgres_to_bigquery_type(&column_schema.typ);
        s.push_str(typ);
        if !column_schema.nullable && !Self::is_array_type(&column_schema.typ) {
            s.push_str(" not null");
        };
    }

    fn add_primary_key_clause(column_schemas: &[ColumnSchema], s: &mut String) {
        let identity_columns = column_schemas.iter().filter(|s| s.primary);

        s.push_str("primary key (");

        for column in identity_columns {
            s.push('`');
            s.push_str(&column.name);
            s.push('`');
            s.push(',');
        }

        s.pop(); //','
        s.push_str(") not enforced");
    }

    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let mut s = String::new();
        s.push('(');

        for column_schema in column_schemas.iter() {
            Self::column_spec(column_schema, &mut s);
            s.push(',');
        }

        let has_identity_cols = column_schemas.iter().any(|s| s.primary);
        if has_identity_cols {
            Self::add_primary_key_clause(column_schemas, &mut s);
        } else {
            s.pop(); //','
        }

        s.push(')');

        s
    }

    fn max_staleness_option(max_staleness_mins: u16) -> String {
        format!("options (max_staleness = interval {max_staleness_mins} minute)")
    }

    pub async fn create_table(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: u16,
    ) -> Result<(), BQError> {
        let columns_spec = Self::create_columns_spec(column_schemas);
        let max_staleness_option = Self::max_staleness_option(max_staleness_mins);
        let project_id = &self.project_id;
        info!("creating table {project_id}.{dataset_id}.{table_name} in bigquery");
        let query = format!(
            "create table `{project_id}.{dataset_id}.{table_name}` {columns_spec} {max_staleness_option}",
        );
        let _ = self.query(query).await?;
        Ok(())
    }

    pub async fn get_default_stream(
        &mut self,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<WriteStream, BQError> {
        let stream_name = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_name.to_string(),
        );
        let write_stream = self
            .client
            .storage_mut()
            .get_write_stream(&stream_name, WriteStreamView::Full)
            .await?;
        Ok(write_stream)
    }

    pub async fn table_exists(&self, dataset_id: &str, table_name: &str) -> Result<bool, BQError> {
        let query = format!(
            "select exists
                (
                    select * from
                    {dataset_id}.INFORMATION_SCHEMA.TABLES
                    where table_name = '{table_name}'
                ) as table_exists;",
        );

        let mut rs = self.query(query).await?;

        let mut exists = false;

        if rs.next_row() {
            exists = rs
                .get_bool_by_name("table_exists")?
                .expect("no column named `table_exists` found in query result");
        }

        Ok(exists)
    }

    pub async fn get_last_lsn(&self, dataset_id: &str) -> Result<PgLsn, BQError> {
        let project_id = &self.project_id;
        let query = format!("select lsn from `{project_id}.{dataset_id}.last_lsn`",);

        let mut rs = self.query(query).await?;

        let lsn: i64 = if rs.next_row() {
            rs.get_i64_by_name("lsn")?
                .expect("no column named `lsn` found in query result")
        } else {
            //TODO: return error instead of panicking
            panic!("failed to get lsn");
        };

        Ok((lsn as u64).into())
    }

    pub async fn set_last_lsn(&self, dataset_id: &str, lsn: PgLsn) -> Result<(), BQError> {
        let lsn: u64 = lsn.into();

        let project_id = &self.project_id;
        let query =
            format!("update `{project_id}.{dataset_id}.last_lsn` set lsn = {lsn} where id = 1",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn insert_last_lsn_row(&self, dataset_id: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query =
            format!("insert into `{project_id}.{dataset_id}.last_lsn` (id, lsn) values (1, 0)",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn get_copied_table_ids(
        &self,
        dataset_id: &str,
    ) -> Result<HashSet<TableId>, BQError> {
        let project_id = &self.project_id;
        let query = format!("select table_id from `{project_id}.{dataset_id}.copied_tables`",);

        let mut rs = self.query(query).await?;
        let mut table_ids = HashSet::new();
        while rs.next_row() {
            let table_id = rs
                .get_i64_by_name("table_id")?
                .expect("no column named `table_id` found in query result");
            table_ids.insert(table_id as TableId);
        }

        Ok(table_ids)
    }

    pub async fn insert_into_copied_tables(
        &self,
        dataset_id: &str,
        table_id: TableId,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query = format!(
            "insert into `{project_id}.{dataset_id}.copied_tables` (table_id) values ({table_id})",
        );

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn insert_row(
        &self,
        dataset_id: &str,
        table_name: &str,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let query = self.create_insert_row_query(dataset_id, table_name, table_row);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn stream_rows(
        &mut self,
        dataset_id: &str,
        table_name: String,
        table_descriptor: &TableDescriptor,
        mut table_rows: &[TableRow],
    ) -> Result<(), BQError> {
        let default_stream = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_name.to_string(),
        );

        const MAX_SIZE_BYTES: usize = 9 * 1024 * 1024; // 9 MB

        loop {
            let (rows, num_processed_rows) =
                StorageApi::create_rows(table_descriptor, table_rows, MAX_SIZE_BYTES);
            let trace_id = "etl bigquery client".to_string();
            let mut response_stream = self
                .client
                .storage_mut()
                .append_rows(&default_stream, rows, trace_id)
                .await?;

            if let Some(r) = response_stream.next().await {
                let _ = r?;
            }

            table_rows = &table_rows[num_processed_rows..];
            if table_rows.is_empty() {
                break;
            }
        }

        Ok(())
    }

    pub async fn insert_rows(
        &self,
        dataset_id: &str,
        table_name: &str,
        insert_request: TableDataInsertAllRequest,
    ) -> Result<(), BQError> {
        self.client
            .tabledata()
            .insert_all(&self.project_id, dataset_id, table_name, insert_request)
            .await?;
        Ok(())
    }

    fn create_insert_row_query(
        &self,
        dataset_id: &str,
        table_name: &str,
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        let project_id = &self.project_id;
        s.push_str(&format!(
            "insert into `{project_id}.{dataset_id}.{table_name}`"
        ));
        s.push_str(" values(");

        for (i, value) in table_row.values.iter().enumerate() {
            Self::cell_to_query_value(value, &mut s);

            if i < table_row.values.len() - 1 {
                s.push(',');
            }
        }

        s.push(')');

        s
    }

    fn cell_to_query_value(cell: &Cell, s: &mut String) {
        match cell {
            Cell::Null(_) => s.push_str("null"),
            Cell::Bool(b) => s.push_str(&format!("{b}")),
            Cell::String(str) => s.push_str(&format!("'{str}'")),
            Cell::I16(i) => s.push_str(&format!("{i}")),
            Cell::I32(i) => s.push_str(&format!("{i}")),
            Cell::I64(i) => s.push_str(&format!("{i}")),
            Cell::F32(i) => s.push_str(&format!("{i}")),
            Cell::F64(i) => s.push_str(&format!("{i}")),
            Cell::Numeric(n) => s.push_str(&format!("{n}")),
            Cell::Date(t) => s.push_str(&format!("'{t}'")),
            Cell::Time(t) => s.push_str(&format!("'{t}'")),
            Cell::TimeStamp(t) => s.push_str(&format!("'{t}'")),
            Cell::TimeStampTz(t) => s.push_str(&format!("'{t}'")),
            Cell::Uuid(t) => s.push_str(&format!("'{t}'")),
            Cell::Json(j) => s.push_str(&format!("'{j}'")),
            Cell::U32(u) => s.push_str(&format!("{u}")),
            Cell::Bytes(b) => {
                let bytes: String = b.iter().map(|b| *b as char).collect();
                s.push_str(&format!("b'{bytes}'"))
            }
            Cell::Array(_) => unreachable!(),
        }
    }

    pub async fn update_row(
        &self,
        dataset_id: &str,
        table_schema: &TableSchema,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let table_name = &table_schema.name.name;
        let table_name = &format!("`{project_id}.{dataset_id}.{table_name}`");
        let column_schemas = &table_schema.column_schemas;
        let query = Self::create_update_row_query(table_name, column_schemas, table_row);
        let _ = self.query(query).await?;
        Ok(())
    }

    fn create_update_row_query(
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        s.push_str("update ");
        s.push_str(table_name);
        s.push_str(" set ");

        let mut remove_comma = false;

        for (cell, column) in table_row.values.iter().zip(column_schemas) {
            if !column.primary {
                s.push_str(&column.name);
                s.push_str(" = ");
                Self::cell_to_query_value(cell, &mut s);
                s.push(',');
                remove_comma = true;
            }
        }

        if remove_comma {
            s.pop();
        }

        Self::add_identities_where_clause(&mut s, column_schemas, table_row);

        s
    }

    /// Adds a where clause for the identity columns
    fn add_identities_where_clause(
        s: &mut String,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) {
        s.push_str(" where ");

        let mut remove_and = false;
        for (cell, column) in table_row.values.iter().zip(column_schemas) {
            if column.primary {
                s.push_str(&column.name);
                s.push_str(" = ");
                Self::cell_to_query_value(cell, s);
                s.push_str(" and ");
                remove_and = true;
            }
        }

        if remove_and {
            s.pop(); //' '
            s.pop(); //'d'
            s.pop(); //'n'
            s.pop(); //'a'
            s.pop(); //' '
        }
    }

    pub async fn delete_row(
        &self,
        dataset_id: &str,
        table_schema: &TableSchema,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let table_name = &table_schema.name.name;
        let table_name = &format!("`{project_id}.{dataset_id}.{table_name}`");
        let column_schemas = &table_schema.column_schemas;
        let query = Self::create_delete_row_query(table_name, column_schemas, table_row);
        let _ = self.query(query).await?;

        Ok(())
    }

    fn create_delete_row_query(
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        s.push_str("delete from ");
        s.push_str(table_name);

        Self::add_identities_where_clause(&mut s, column_schemas, table_row);

        s
    }

    pub async fn drop_table(&self, dataset_id: &str, table_name: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        info!("dropping table {project_id}.{dataset_id}.{table_name} in bigquery");
        let query = format!("drop table `{project_id}.{dataset_id}.{table_name}`",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn begin_transaction(&self) -> Result<(), BQError> {
        let _ = self.query("begin transaction".to_string()).await?;

        Ok(())
    }

    pub async fn commit_transaction(&self) -> Result<(), BQError> {
        let _ = self.query("commit".to_string()).await?;

        Ok(())
    }

    pub async fn set_last_lsn_and_commit_transaction(
        &self,
        dataset_id: &str,
        last_lsn: PgLsn,
    ) -> Result<(), BQError> {
        self.set_last_lsn(dataset_id, last_lsn).await?;
        self.commit_transaction().await?;
        Ok(())
    }

    async fn query(&self, query: String) -> Result<ResultSet, BQError> {
        let query_response = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;
        Ok(ResultSet::new_from_query_response(query_response))
    }
}

/// Converts a [`TableSchema`] to [`TableDescriptor`].
///
/// This function is defined here and doesn't use the [`From`] trait because it's not possible since
/// [`TableSchema`] is in another crate and we don't want to pollute the `postgres` crate with destination
/// specific internals.
pub fn table_schema_to_descriptor(table_schema: &TableSchema) -> TableDescriptor {
    let mut field_descriptors = Vec::with_capacity(table_schema.column_schemas.len());
    let mut number = 1;
    for column_schema in &table_schema.column_schemas {
        let typ = match column_schema.typ {
            Type::BOOL => ColumnType::Bool,
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                ColumnType::String
            }
            Type::INT2 => ColumnType::Int32,
            Type::INT4 => ColumnType::Int32,
            Type::INT8 => ColumnType::Int64,
            Type::FLOAT4 => ColumnType::Float,
            Type::FLOAT8 => ColumnType::Double,
            Type::NUMERIC => ColumnType::String,
            Type::DATE => ColumnType::String,
            Type::TIME => ColumnType::String,
            Type::TIMESTAMP => ColumnType::String,
            Type::TIMESTAMPTZ => ColumnType::String,
            Type::UUID => ColumnType::String,
            Type::JSON => ColumnType::String,
            Type::JSONB => ColumnType::String,
            Type::OID => ColumnType::Int32,
            Type::BYTEA => ColumnType::Bytes,
            Type::BOOL_ARRAY => ColumnType::Bool,
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => ColumnType::String,
            Type::INT2_ARRAY => ColumnType::Int32,
            Type::INT4_ARRAY => ColumnType::Int32,
            Type::INT8_ARRAY => ColumnType::Int64,
            Type::FLOAT4_ARRAY => ColumnType::Float,
            Type::FLOAT8_ARRAY => ColumnType::Double,
            Type::NUMERIC_ARRAY => ColumnType::String,
            Type::DATE_ARRAY => ColumnType::String,
            Type::TIME_ARRAY => ColumnType::String,
            Type::TIMESTAMP_ARRAY => ColumnType::String,
            Type::TIMESTAMPTZ_ARRAY => ColumnType::String,
            Type::UUID_ARRAY => ColumnType::String,
            Type::JSON_ARRAY => ColumnType::String,
            Type::JSONB_ARRAY => ColumnType::String,
            Type::OID_ARRAY => ColumnType::Int32,
            Type::BYTEA_ARRAY => ColumnType::Bytes,
            _ => ColumnType::String,
        };

        let mode = match column_schema.typ {
            Type::BOOL_ARRAY
            | Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY
            | Type::INT2_ARRAY
            | Type::INT4_ARRAY
            | Type::INT8_ARRAY
            | Type::FLOAT4_ARRAY
            | Type::FLOAT8_ARRAY
            | Type::NUMERIC_ARRAY
            | Type::DATE_ARRAY
            | Type::TIME_ARRAY
            | Type::TIMESTAMP_ARRAY
            | Type::TIMESTAMPTZ_ARRAY
            | Type::UUID_ARRAY
            | Type::JSON_ARRAY
            | Type::JSONB_ARRAY
            | Type::OID_ARRAY
            | Type::BYTEA_ARRAY => ColumnMode::Repeated,
            _ => {
                if column_schema.nullable {
                    ColumnMode::Nullable
                } else {
                    ColumnMode::Required
                }
            }
        };

        field_descriptors.push(FieldDescriptor {
            number,
            name: column_schema.name.clone(),
            typ,
            mode,
        });
        number += 1;
    }

    field_descriptors.push(FieldDescriptor {
        number,
        name: "_CHANGE_TYPE".to_string(),
        typ: ColumnType::String,
        mode: ColumnMode::Required,
    });

    TableDescriptor { field_descriptors }
}
