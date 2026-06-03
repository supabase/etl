pub mod catalog;
mod db;
pub mod destination_table_metadata;
pub mod health;
pub mod lag;
pub mod progress;
pub mod schema;
pub mod slots;
pub mod table_state;

pub use db::{
    TableLookupError, connect_to_source_database, extract_server_version,
    get_table_names_from_table_ids,
};
