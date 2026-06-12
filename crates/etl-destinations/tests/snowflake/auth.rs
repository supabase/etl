use etl_destinations::snowflake::{AuthManager, TokenProvider, test_utils::load_test_config};

#[tokio::test]
#[ignore = "requires Snowflake credentials — see etl-destinations/src/snowflake/README.md"]
async fn authenticate_against_snowflake() {
    let auth = AuthManager::new(load_test_config()).expect("AuthManager creation failed");

    let token = auth.get_token().await.expect("authentication failed");
    assert!(!token.is_empty(), "token should not be empty");

    let token2 = auth.get_token().await.expect("second get_token failed");
    assert_eq!(token, token2, "expected cached token on second call");
}
