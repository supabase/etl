//! Bijection's narrow adapter to upstream transactional schema messages.
//! Source capture itself is owned by the `postgres_capture` crate.
use crate::{error::EtlResult, postgres::codec::SchemaChangeMessage};

pub const DDL_MESSAGE_PREFIX: &str = crate::postgres::codec::DDL_MESSAGE_PREFIX;

/// The affected physical table, only when the message applies to this
/// publication.
pub fn affected_table(content: &str, publication: &str) -> EtlResult<Option<u32>> {
    let message: SchemaChangeMessage = content.parse()?;
    Ok(message.applies_to_publication(publication).then_some(message.oid))
}
