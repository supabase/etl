use etl_api::routes::images::{CreateImageRequest, CreateImageResponse};
use etl_api::routes::images::{ReadImageResponse, ReadImagesResponse, UpdateImageRequest};

mod support;

use crate::support::mocks::{create_default_image, create_image_with_name};
use crate::support::test_app::spawn_test_app;

#[tokio::test(flavor = "multi_thread")]
async fn images_default_image_can_be_updated_via_endpoint() {
    let app = spawn_test_app().await;

    let _old_default = create_default_image(&app).await;
    // Act: set the new image as default by name (creates if missing)
    let response = app
        .set_default_image_by_name("supabase/etl-replicator:2.0.0")
        .await;
    assert!(response.status().is_success());

    // Assert: only the requested image is now default
    let response = app.read_all_images().await;
    assert!(response.status().is_success());
    let body: ReadImagesResponse = response.json().await.expect("failed to deserialize images");

    let defaults: Vec<_> = body.images.iter().filter(|i| i.is_default).collect();
    assert_eq!(defaults.len(), 1, "exactly one default image must exist");
    assert_eq!(defaults[0].name, "supabase/etl-replicator:2.0.0");
}

#[tokio::test(flavor = "multi_thread")]
async fn images_default_endpoint_creates_when_missing() {
    let app = spawn_test_app().await;

    // No images created; setting default by name should create it
    let response = app.set_default_image_by_name("brand/new:1.0.0").await;
    assert!(response.status().is_success());

    let response = app.read_all_images().await;
    let body: ReadImagesResponse = response.json().await.expect("deserialize images");
    let defaults: Vec<_> = body.images.iter().filter(|i| i.is_default).collect();
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0].name, "brand/new:1.0.0");
}

#[tokio::test(flavor = "multi_thread")]
async fn images_create_and_read_works() {
    let app = spawn_test_app().await;

    let image_id =
        create_image_with_name(&app, "supabase/etl-replicator:1.0.0".to_string(), false).await;

    // Read by id
    let response = app.read_image(image_id).await;
    assert!(response.status().is_success());
    let body: ReadImageResponse = response.json().await.expect("failed to deserialize image");
    assert_eq!(body.id, image_id);
    assert_eq!(body.name, "supabase/etl-replicator:1.0.0");
    assert!(!body.is_default);
}

#[tokio::test(flavor = "multi_thread")]
async fn images_read_missing_returns_404() {
    let app = spawn_test_app().await;
    let response = app.read_image(999999).await;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn images_update_can_rename_and_flip_default() {
    let app = spawn_test_app().await;

    // Start with an existing default
    let default_id = create_default_image(&app).await;
    // Create a non-default
    let other_id =
        create_image_with_name(&app, "supabase/etl-replicator:2.0.0".to_string(), false).await;

    // Flip default to the other image and rename it
    let update = UpdateImageRequest {
        name: "supabase/etl-replicator:2.1.0".to_string(),
        is_default: true,
    };
    let response = app.update_image(other_id, &update).await;
    assert!(response.status().is_success());

    // Check that only other_id is default now, and name updated
    let response = app.read_all_images().await;
    assert!(response.status().is_success());
    let body: ReadImagesResponse = response.json().await.expect("failed to deserialize images");
    let defaults: Vec<_> = body.images.iter().filter(|i| i.is_default).collect();
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0].id, other_id);

    // Old default should not be default anymore
    let old = body.images.iter().find(|i| i.id == default_id).unwrap();
    assert!(!old.is_default);

    // New default name updated
    let new = body.images.iter().find(|i| i.id == other_id).unwrap();
    assert_eq!(new.name, "supabase/etl-replicator:2.1.0");
}

#[tokio::test(flavor = "multi_thread")]
async fn images_delete_prevents_deleting_default() {
    let app = spawn_test_app().await;
    let default_id = create_default_image(&app).await;

    let response = app.delete_image(default_id).await;
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
}

#[tokio::test(flavor = "multi_thread")]
async fn images_delete_non_default_succeeds() {
    let app = spawn_test_app().await;
    let _default_id = create_default_image(&app).await;
    let non_default_id =
        create_image_with_name(&app, "supabase/etl-replicator:9.9.9".to_string(), false).await;

    let response = app.delete_image(non_default_id).await;
    assert!(response.status().is_success());

    // Verify it's gone
    let response = app.read_image(non_default_id).await;
    assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn images_list_returns_all() {
    let app = spawn_test_app().await;
    let a = create_image_with_name(&app, "a".to_string(), false).await;
    let b = create_image_with_name(&app, "b".to_string(), false).await;

    let response = app.read_all_images().await;
    assert!(response.status().is_success());
    let body: ReadImagesResponse = response.json().await.expect("failed to deserialize images");
    let ids: Vec<i64> = body.images.iter().map(|i| i.id).collect();
    assert!(ids.contains(&a) && ids.contains(&b));
}

#[tokio::test(flavor = "multi_thread")]
async fn images_create_endpoint_can_create_non_default_and_read() {
    let app = spawn_test_app().await;
    // Ensure there is a default image already present
    let _ = create_default_image(&app).await;

    let req = CreateImageRequest {
        name: "endpoint/create:1.0.0".to_string(),
        is_default: false,
    };
    let resp = app.create_image(&req).await;
    assert!(resp.status().is_success());
    let CreateImageResponse { id } = resp
        .json()
        .await
        .expect("failed to deserialize create response");

    // Read it back and validate fields
    let response = app.read_image(id).await;
    assert!(response.status().is_success());
    let body: ReadImageResponse = response.json().await.expect("deserialize image");
    assert_eq!(body.id, id);
    assert_eq!(body.name, "endpoint/create:1.0.0");
    assert!(!body.is_default);
}

#[tokio::test(flavor = "multi_thread")]
async fn images_create_endpoint_can_create_and_flip_default() {
    let app = spawn_test_app().await;
    let _old_default_id = create_default_image(&app).await;

    let req = CreateImageRequest {
        name: "endpoint/create:2.0.0".to_string(),
        is_default: true,
    };
    let resp = app.create_image(&req).await;
    assert!(resp.status().is_success());
    let CreateImageResponse { id: new_id } = resp
        .json()
        .await
        .expect("failed to deserialize create response");

    // Assert exactly one default and it's the newly created image
    let response = app.read_all_images().await;
    assert!(response.status().is_success());
    let body: ReadImagesResponse = response.json().await.expect("deserialize images");
    let defaults: Vec<_> = body.images.iter().filter(|i| i.is_default).collect();
    assert_eq!(defaults.len(), 1);
    assert_eq!(defaults[0].id, new_id);
}
