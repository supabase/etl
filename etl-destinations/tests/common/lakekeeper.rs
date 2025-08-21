pub struct LakekeeperClient {
    client: reqwest::Client,
}

impl LakekeeperClient {
    pub fn new() -> Self {
        LakekeeperClient {
            client: reqwest::Client::new(),
        }
    }
}
