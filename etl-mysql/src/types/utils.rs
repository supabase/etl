/// Returns whether the MySQL type is an array-like type.
///
/// Note: MySQL doesn't have native array types like PostgreSQL, but JSON arrays
/// can be used for similar functionality.
pub fn is_array_type(typ: &str) -> bool {
    matches!(typ, "JSON" | "json")
}

/// Creates a hex-encoded sequence number from binlog positions to ensure correct event ordering.
///
/// Creates a hex-encoded sequence number that ensures events are processed in the correct order
/// even when they have the same system time. The format is compatible with BigQuery's
/// `_CHANGE_SEQUENCE_NUMBER` column requirements.
///
/// The rationale for using the binlog position is that BigQuery will preserve the highest sequence
/// number in case of equal primary key, which is what we want since in case of updates, we want the
/// latest update in MySQL order to be the winner. We have first the `commit_pos` in the key
/// so that BigQuery can first order operations based on the position at which the transaction committed
/// and if two operations belong to the same transaction (meaning they have the same position), the
/// `start_pos` will be used. We first order by `commit_pos` to preserve the order in which operations
/// are received by the pipeline since transactions are ordered by commit time and not interleaved.
pub fn generate_sequence_number(start_pos: u64, commit_pos: u64) -> String {
    format!("{commit_pos:016x}/{start_pos:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_array_type() {
        assert!(is_array_type("JSON"));
        assert!(is_array_type("json"));
        assert!(!is_array_type("VARCHAR"));
        assert!(!is_array_type("INT"));
        assert!(!is_array_type("TEXT"));
    }

    #[test]
    fn test_generate_sequence_number() {
        let start = 1000u64;
        let commit = 2000u64;
        let seq = generate_sequence_number(start, commit);
        assert_eq!(seq, "00000000000007d0/00000000000003e8");
    }

    #[test]
    fn test_generate_sequence_number_ordering() {
        let seq1 = generate_sequence_number(100, 1000);
        let seq2 = generate_sequence_number(200, 1000);
        let seq3 = generate_sequence_number(100, 2000);

        assert!(seq2 > seq1);
        assert!(seq3 > seq1);
        assert!(seq3 > seq2);
    }
}
