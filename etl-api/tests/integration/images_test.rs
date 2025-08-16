use etl_api::routes::images::{
    CreateImageRequest, CreateImageResponse, ReadImageResponse, ReadImagesResponse,
    UpdateImageRequest,
};
use etl_telemetry::tracing::init_test_tracing;
use reqwest::StatusCode;

use crate::common::test_app::{TestApp, spawn_test_app};

pub async fn create_default_image(app: &TestApp) -> i64 {
    create_image_with_name(app, "some/image".to_string(), true).await
}

pub async fn create_image_with_name(app: &TestApp, name: String, is_default: bool) -> i64 {
    let image = CreateImageRequest { name, is_default };
    let response = app.create_image(&image).await;
    let response: CreateImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    response.id
}

#[tokio::test(flavor = "multi_thread")]
async fn image_can_be_created() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let image = CreateImageRequest {
        name: "some/image".to_string(),
        is_default: true,
    };
    let response = app.create_image(&image).await;

    // Assert
    assert!(response.status().is_success());
    let response: CreateImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_image_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let name = "some/image".to_string();
    let is_default = true;
    let image = CreateImageRequest {
        name: name.clone(),
        is_default,
    };
    let response = app.create_image(&image).await;
    let response: CreateImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let image_id = response.id;

    // Act
    let response = app.read_image(image_id).await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, image_id);
    assert_eq!(response.name, name);
    assert_eq!(response.is_default, is_default);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_image_cant_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let response = app.read_image(42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_image_can_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let name = "some/image".to_string();
    let is_default = true;
    let image = CreateImageRequest {
        name: name.clone(),
        is_default,
    };
    let response = app.create_image(&image).await;
    let response: CreateImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let image_id = response.id;

    // Act
    let name = "some/image".to_string();
    let is_default = true;
    let updated_image = UpdateImageRequest {
        name: name.clone(),
        is_default,
    };
    let response = app.update_image(image_id, &updated_image).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_image(image_id).await;
    let response: ReadImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    assert_eq!(response.id, image_id);
    assert_eq!(response.name, name);
    assert_eq!(response.is_default, is_default);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_source_cant_be_updated() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let name = "some/image".to_string();
    let is_default = true;
    let updated_image = UpdateImageRequest {
        name: name.clone(),
        is_default,
    };
    let response = app.update_image(42, &updated_image).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_existing_image_can_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let name = "some/image".to_string();
    let is_default = false;
    let image = CreateImageRequest {
        name: name.clone(),
        is_default,
    };
    let response = app.create_image(&image).await;
    let response: CreateImageResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    let image_id = response.id;

    // Act
    let response = app.delete_image(image_id).await;

    // Assert
    assert!(response.status().is_success());
    let response = app.read_image(image_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_non_existing_image_cant_be_deleted() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Act
    let response = app.delete_image(42).await;

    // Assert
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn all_images_can_be_read() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;
    let image1_id = create_image_with_name(&app, "some/image".to_string(), true).await;
    let image2_id = create_image_with_name(&app, "other/image".to_string(), false).await;

    // Act
    let response = app.read_all_images().await;

    // Assert
    assert!(response.status().is_success());
    let response: ReadImagesResponse = response
        .json()
        .await
        .expect("failed to deserialize response");
    for image in response.images {
        if image.id == image1_id {
            assert_eq!(image.name, "some/image");
            assert!(image.is_default);
        } else if image.id == image2_id {
            assert_eq!(image.name, "other/image");
            assert!(!image.is_default);
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn creating_default_image_switches_previous_default() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Create first default image
    let image1 = CreateImageRequest {
        name: "image1".to_string(),
        is_default: true,
    };
    let response = app.create_image(&image1).await;
    assert!(response.status().is_success());
    let response: CreateImageResponse = response.json().await.expect("failed to deserialize");
    let image1_id = response.id;

    // Act - Create second default image
    let image2 = CreateImageRequest {
        name: "image2".to_string(),
        is_default: true,
    };
    let response = app.create_image(&image2).await;
    assert!(response.status().is_success());
    let response: CreateImageResponse = response.json().await.expect("failed to deserialize");
    let image2_id = response.id;

    // Assert - First image should no longer be default, second should be default
    let response = app.read_image(image1_id).await;
    let image1: ReadImageResponse = response.json().await.expect("failed to deserialize");
    assert!(!image1.is_default);

    let response = app.read_image(image2_id).await;
    let image2: ReadImageResponse = response.json().await.expect("failed to deserialize");
    assert!(image2.is_default);
}

#[tokio::test(flavor = "multi_thread")]
async fn cannot_delete_default_image() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let image = CreateImageRequest {
        name: "default-image".to_string(),
        is_default: true,
    };
    let response = app.create_image(&image).await;
    let response: CreateImageResponse = response.json().await.expect("failed to deserialize");
    let image_id = response.id;

    // Act - Try to delete the default image
    let response = app.delete_image(image_id).await;

    // Assert - Should fail with 400 Bad Request
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    // Verify the image still exists
    let response = app.read_image(image_id).await;
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn can_delete_non_default_image_after_switching_default() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Create first default image
    let image1 = CreateImageRequest {
        name: "default-image-1".to_string(),
        is_default: true,
    };
    let response = app.create_image(&image1).await;
    let response: CreateImageResponse = response.json().await.expect("failed to deserialize");
    let image1_id = response.id;

    // Create second non-default image
    let image2 = CreateImageRequest {
        name: "non-default-image".to_string(),
        is_default: false,
    };
    let response = app.create_image(&image2).await;
    let response: CreateImageResponse = response.json().await.expect("failed to deserialize");
    let image2_id = response.id;

    // Make the second image default (this should succeed and make the first one non-default)
    let update = UpdateImageRequest {
        name: "non-default-image".to_string(),
        is_default: true,
    };
    let response = app.update_image(image2_id, &update).await;
    assert!(response.status().is_success());

    // Act - Now delete the first image (which should no longer be default)
    let response = app.delete_image(image1_id).await;

    // Assert - Should succeed since the first image is no longer default
    assert!(response.status().is_success());

    // Verify the first image is deleted but the second (default) remains
    let response = app.read_image(image1_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let response = app.read_image(image2_id).await;
    assert!(response.status().is_success());
}

#[tokio::test(flavor = "multi_thread")]
async fn can_update_default_image_to_non_default() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    let image = CreateImageRequest {
        name: "default-image".to_string(),
        is_default: true,
    };
    let response = app.create_image(&image).await;
    let response: CreateImageResponse = response.json().await.expect("failed to deserialize");
    let image_id = response.id;

    // Act - Update the image to non-default
    let update = UpdateImageRequest {
        name: "default-image-updated".to_string(),
        is_default: false,
    };
    let response = app.update_image(image_id, &update).await;

    // Assert - Should succeed
    assert!(response.status().is_success());

    // Verify the update
    let response = app.read_image(image_id).await;
    let image: ReadImageResponse = response.json().await.expect("failed to deserialize");
    assert_eq!(image.name, "default-image-updated");
    assert!(!image.is_default);
}

#[tokio::test(flavor = "multi_thread")]
async fn can_delete_non_default_image() {
    init_test_tracing();
    // Arrange
    let app = spawn_test_app().await;

    // Create a default image first
    let default_image = CreateImageRequest {
        name: "default-image".to_string(),
        is_default: true,
    };
    let response = app.create_image(&default_image).await;
    assert!(response.status().is_success());

    // Create a non-default image
    let non_default_image = CreateImageRequest {
        name: "non-default-image".to_string(),
        is_default: false,
    };
    let response = app.create_image(&non_default_image).await;
    let response: CreateImageResponse = response.json().await.expect("failed to deserialize");
    let non_default_id = response.id;

    // Act - Delete the non-default image
    let response = app.delete_image(non_default_id).await;

    // Assert - Should succeed
    assert!(response.status().is_success());
    let response = app.read_image(non_default_id).await;
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
