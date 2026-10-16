use anyhow::Context;

/// Bound every allocation-driving count before the upstream protocol parser.
/// Its signed counts must not become huge usize capacities. Binary tuples are
/// not negotiated; text values are sliced only inside their checked frame.
pub(crate) fn validate_allocations(data: &[u8]) -> anyhow::Result<()> {
    fn tuple(data: &[u8], position: &mut usize) -> anyhow::Result<()> {
        let count = data.get(*position..*position + 2).context("tuple column count missing")?;
        let count = u16::from_be_bytes(count.try_into()?);
        *position += 2;
        anyhow::ensure!(
            i16::try_from(count).is_ok()
                && usize::from(count) <= data.len().saturating_sub(*position),
            "tuple count exceeds its frame"
        );
        for _ in 0..count {
            let tag = *data.get(*position).context("tuple cell missing")?;
            *position += 1;
            match tag {
                b'n' | b'u' => {}
                b't' => {
                    let length =
                        data.get(*position..*position + 4).context("tuple length missing")?;
                    let length = i32::from_be_bytes(length.try_into()?);
                    *position += 4;
                    anyhow::ensure!(length >= 0, "negative tuple length");
                    *position =
                        position.checked_add(length as usize).context("tuple length overflow")?;
                    anyhow::ensure!(*position <= data.len(), "tuple exceeds its frame");
                }
                b'b' => anyhow::bail!("binary tuple output was not negotiated"),
                _ => anyhow::bail!("unsupported tuple cell encoding"),
            }
        }
        Ok(())
    }
    match data.first() {
        Some(b'R') => {
            let mut position = 5;
            for _ in 0..2 {
                let tail = data.get(position..).context("relation name missing")?;
                position += tail
                    .iter()
                    .position(|byte| *byte == 0)
                    .context("unterminated relation name")?
                    + 1;
            }
            position += 1; // replica identity
            let count = i16::from_be_bytes(
                data.get(position..position + 2)
                    .context("relation column count missing")?
                    .try_into()?,
            );
            position += 2;
            // flags, at least a terminating name byte, type OID and typmod.
            anyhow::ensure!(
                count >= 0 && count as usize <= data.len().saturating_sub(position) / 10,
                "relation count exceeds its frame"
            );
        }
        Some(b'T') => {
            let count =
                i32::from_be_bytes(data.get(1..5).context("truncate count missing")?.try_into()?);
            anyhow::ensure!(
                count >= 0
                    && data.len() >= 6
                    && count as usize == (data.len() - 6) / 4
                    && (data.len() - 6).is_multiple_of(4),
                "truncate count exceeds its frame"
            );
        }
        _ => {}
    }
    if !matches!(data.first(), Some(b'I' | b'U' | b'D')) {
        return Ok(());
    }
    let mut position = 5;
    let marker = *data.get(position).context("tuple marker missing")?;
    position += 1;
    anyhow::ensure!(matches!(marker, b'N' | b'O' | b'K'), "invalid tuple marker");
    tuple(data, &mut position)?;
    if data[0] == b'U' && marker != b'N' {
        anyhow::ensure!(data.get(position) == Some(&b'N'), "update replacement tuple missing");
        position += 1;
        tuple(data, &mut position)?;
    }
    anyhow::ensure!(position == data.len(), "tuple has unexpected trailing bytes");
    Ok(())
}

/// Both synchronization modes use one guarded logical-message decoder.
pub(crate) fn parse(
    data: &bytes::Bytes,
) -> anyhow::Result<postgres_replication::protocol::LogicalReplicationMessage> {
    validate_allocations(data)?;
    Ok(postgres_replication::protocol::LogicalReplicationMessage::parse(data)?)
}
