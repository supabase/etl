//! Validators for destination identifiers that are interpolated into URLs.

use super::ValidationError;

const SNOWFLAKE_ACCOUNT_ID_MAX_LEN: usize = 63;
const SUPABASE_PROJECT_REF_LEN: usize = 20;

/// Validates that a Snowflake account identifier is in the preferred
/// `orgname-accountname` format.
///
/// See https://docs.snowflake.com/en/user-guide/admin-account-identifier for details.
pub fn validate_snowflake_account_id(account_id: &str) -> Result<(), ValidationError> {
    let invalid = || ValidationError::InvalidFieldValue {
        field: "account_id".to_owned(),
        constraint: "must be a valid Snowflake account identifier in orgname-accountname format \
                     (e.g. MYORG-MYACCOUNT): orgname is ASCII letters and digits starting with a \
                     letter; accountname is ASCII letters, digits and underscores starting with a \
                     letter and not ending with an underscore; max 63 characters"
            .to_owned(),
    };

    if account_id.is_empty() || account_id.len() > SNOWFLAKE_ACCOUNT_ID_MAX_LEN {
        return Err(invalid());
    }

    let Some((org, account)) = account_id.split_once('-') else {
        return Err(invalid());
    };

    if org.is_empty()
        || !org.starts_with(|c: char| c.is_ascii_alphabetic())
        || !org.chars().all(|c| c.is_ascii_alphanumeric())
    {
        return Err(invalid());
    }

    if account.is_empty()
        || !account.starts_with(|c: char| c.is_ascii_alphabetic())
        || !account.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        || account.ends_with('_')
    {
        return Err(invalid());
    }

    Ok(())
}

/// Validates a Supabase project reference.
///
/// A project ref is exactly 20 lowercase ASCII alphanumeric characters forming
/// a single label (no dots).
pub fn validate_supabase_project_ref(project_ref: &str) -> Result<(), ValidationError> {
    let is_valid = project_ref.len() == SUPABASE_PROJECT_REF_LEN
        && project_ref.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit());

    if is_valid {
        Ok(())
    } else {
        Err(ValidationError::InvalidFieldValue {
            field: "project_ref".to_owned(),
            constraint: "must be exactly 20 lowercase alphanumeric characters".to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snowflake_account_id() {
        let at_limit = format!("{}-{}", "a".repeat(31), "b".repeat(31));
        assert_eq!(at_limit.len(), 63);
        let over_limit = format!("{}-{}", "a".repeat(31), "b".repeat(32));
        assert_eq!(over_limit.len(), 64);

        let cases: &[(&str, bool)] = &[
            // Valid org-account identifiers.
            ("myorg-myaccount", true),
            ("ORGNAME-ACCOUNTNAME", true),
            ("org123-my_test_account", true),
            ("a-b", true),
            ("Acme-test_aws_us_east_2", true),
            (&at_limit, true),
            // Empty.
            ("", false),
            // SSRF injection payloads.
            ("127.0.0.1:8443/x", false),
            ("attacker.example/foo", false),
            ("169.254.169.254#", false),
            ("evil@host", false),
            ("https://evil.com", false),
            ("host?x=1", false),
            ("a b", false),
            ("abc%2f", false),
            // Legacy dotted locators are not accepted.
            ("xy12345.us-east-1.aws", false),
            ("xy12345", false),
            // No hyphen (single part).
            ("abc123", false),
            // Org must start with a letter.
            ("1org-account", false),
            // Account must start with a letter.
            ("org-1account", false),
            // Account must not end with underscore.
            ("org-account_", false),
            // Underscore not allowed in org name.
            ("my_org-acct", false),
            // Dots not allowed.
            ("org-acct.test", false),
            // Empty org or account.
            ("-account", false),
            ("org-", false),
            // Exceeds 63-character limit.
            (&over_limit, false),
        ];

        for (id, expected) in cases {
            let result = validate_snowflake_account_id(id);
            assert_eq!(result.is_ok(), *expected, "account_id {id:?}");
        }
    }

    #[test]
    fn supabase_project_ref() {
        let cases: &[(&str, bool)] = &[
            // Valid project refs.
            ("abcdefghijklmnopqrst", true),
            ("a1b2c3d4e5f6g7h8i9j0", true),
            ("00000000000000000000", true),
            // Empty.
            ("", false),
            // Wrong length.
            ("tooshort", false),
            ("abcdefghijklmnopqrstu", false), // 21 chars
            // Disallowed characters.
            ("ABCDEFGHIJKLMNOPQRST", false), // uppercase
            ("abcdefghij_lmnopqrst", false), // underscore
            ("abcdefghij.lmnopqrst", false), // dot
            // SSRF injection payloads.
            ("attacker.example/foo", false),
            ("169.254.169.254#", false),
            ("127.0.0.1:8443/x123x", false),
        ];

        for (input, expected) in cases {
            let result = validate_supabase_project_ref(input);
            assert_eq!(result.is_ok(), *expected, "project_ref {input:?}");
        }
    }
}
