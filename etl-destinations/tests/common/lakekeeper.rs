use uuid::Uuid;

pub struct LakekeeperClient {
    base_url: String,
    client: reqwest::Client,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum DeleteProfileType {
    Hard,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct DeleteProfile {
    r#type: DeleteProfileType,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum CredentialType {
    AccessKey,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum Type {
    S3,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct StorageCredential {
    aws_access_key_id: String,
    aws_secret_access_key: String,
    credential_type: CredentialType,
    r#type: Type,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
enum Flavor {
    #[serde(rename = "minio")]
    MinIO,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct StorageProfile {
    bucket: String,
    region: String,
    sts_enabled: bool,
    r#type: Type,
    endpoint: String,
    path_style_access: bool,
    flavor: Flavor,
    key_prefix: String,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "kebab-case")]
struct Warehouse {
    delete_profile: DeleteProfile,
    storage_credential: StorageCredential,
    storage_profile: StorageProfile,
    warehouse_name: String,
}

impl Default for Warehouse {
    fn default() -> Self {
        Warehouse {
            delete_profile: DeleteProfile {
                r#type: DeleteProfileType::Hard,
            },
            storage_credential: StorageCredential {
                aws_access_key_id: "minio-admin".to_string(),
                aws_secret_access_key: "minio-admin-password".to_string(),
                credential_type: CredentialType::AccessKey,
                r#type: Type::S3,
            },
            storage_profile: StorageProfile {
                bucket: "dev-and-test".to_string(),
                region: "local-01".to_string(),
                sts_enabled: false,
                r#type: Type::S3,
                endpoint: "http://minio:9000".to_string(),
                path_style_access: true,
                flavor: Flavor::MinIO,
                key_prefix: Uuid::new_v4().to_string(),
            },
            warehouse_name: Uuid::new_v4().to_string(),
        }
    }
}

const PROJECT_ID_HEADER: &str = "x-project-id";
const PROJECT_ID: &str = "00000000-0000-0000-0000-000000000000";

impl LakekeeperClient {
    pub fn new(base_url: &str) -> Self {
        let trailing_slash = if base_url.ends_with('/') { "" } else { "/" };
        LakekeeperClient {
            base_url: format!("{base_url}{trailing_slash}management/v1"),
            client: reqwest::Client::new(),
        }
    }

    /// Creates a new warehouse with a random uuid as name
    pub async fn create_warehouse(&self) -> Result<String, reqwest::Error> {
        let warehouse = Warehouse::default();

        let url = format!("{}/warehouse", self.base_url);

        let response = self
            .client
            .post(url)
            .header(PROJECT_ID_HEADER, PROJECT_ID)
            .json(&warehouse)
            .send()
            .await?;

        response.error_for_status()?;

        Ok(warehouse.warehouse_name)
    }
}
