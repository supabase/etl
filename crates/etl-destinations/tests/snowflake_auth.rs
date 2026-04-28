use etl_destinations::snowflake::{AuthManager, test_utils::load_test_config};

#[tokio::test]
#[ignore = "requires Snowflake credentials — see etl-destinations/src/snowflake/README.md"]
async fn authenticate_against_snowflake() {
    let config = load_test_config();

    let auth = AuthManager::new(
        &config.account,
        &config.user,
        config.private_key_path.to_str().unwrap(),
        None,
        None,
    )
    .expect("AuthManager creation failed");

    let token = auth.get_token().await.expect("authentication failed");
    assert!(!token.is_empty(), "token should not be empty");

    let token2 = auth.get_token().await.expect("second get_token failed");
    assert_eq!(token, token2, "expected cached token on second call");
}
