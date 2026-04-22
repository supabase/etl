mod db;
pub mod destination_metadata;
pub mod health;
pub mod lag;
pub mod schema;
pub mod slots;
pub mod state;

pub use db::{
    TableLookupError, connect_to_source_database, extract_server_version,
    get_table_names_from_table_ids,
};
