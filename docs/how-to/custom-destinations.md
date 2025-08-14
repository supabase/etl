---
type: how-to
audience: developers
prerequisites:
  - Complete first pipeline tutorial
  - Rust async/await knowledge
  - Understanding of your target system's API
version_last_tested: 0.1.0
last_reviewed: 2025-01-14
risk_level: medium
---

# Build Custom Destinations

**Create destination implementations for systems not supported out of the box**

This guide walks you through implementing the [`Destination`](../../reference/destination-trait/) trait to send replicated data to custom storage systems, APIs, or data warehouses.

## Goal

Build a custom destination that receives batched data changes from ETL and writes them to your target system with proper error handling and retry logic.

## Prerequisites

- Completed [first pipeline tutorial](../../tutorials/first-pipeline/)
- Access to your target system (database, API, etc.)
- Understanding of your target system's data ingestion patterns
- Rust knowledge of traits and async programming

## Decision Points

**Choose your approach based on your target system:**

| Target System | Key Considerations | Recommended Pattern |
|---------------|-------------------|-------------------|
| **REST API** | Rate limiting, authentication | Batch with retry backoff |
| **Database** | Transaction support, connection pooling | Bulk insert transactions |
| **File System** | File formats, compression | Append or rotate files |
| **Message Queue** | Ordering guarantees, partitioning | Individual message sending |

## Implementation Steps

### Step 1: Define Your Destination Struct

Create a new file `src/my_destination.rs`:

```rust
use etl::destination::base::{Destination, DestinationError};
use etl::types::pipeline::BatchedData;
use async_trait::async_trait;

pub struct MyCustomDestination {
    // Configuration fields
    api_endpoint: String,
    auth_token: String,
    batch_size: usize,
}

impl MyCustomDestination {
    pub fn new(api_endpoint: String, auth_token: String) -> Self {
        Self {
            api_endpoint,
            auth_token,
            batch_size: 1000,
        }
    }
}
```

### Step 2: Implement the Destination Trait

Add the core trait implementation:

```rust
#[async_trait]
impl Destination for MyCustomDestination {
    async fn write_batch(&mut self, batch: BatchedData) -> Result<(), DestinationError> {
        // Convert ETL data to your target format
        let payload = self.convert_batch_to_target_format(&batch)?;
        
        // Send to your target system with retries
        self.send_with_retries(payload).await?;
        
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), DestinationError> {
        // Implement any final cleanup or flush logic
        Ok(())
    }
}
```

### Step 3: Implement Data Conversion

Add conversion logic specific to your target system:

```rust
impl MyCustomDestination {
    fn convert_batch_to_target_format(&self, batch: &BatchedData) -> Result<String, DestinationError> {
        let mut records = Vec::new();
        
        for change in &batch.changes {
            match change.operation {
                Operation::Insert => {
                    records.push(json!({
                        "action": "insert",
                        "table": change.table_name,
                        "data": change.new_values,
                        "timestamp": change.timestamp
                    }));
                }
                Operation::Update => {
                    records.push(json!({
                        "action": "update", 
                        "table": change.table_name,
                        "old_data": change.old_values,
                        "new_data": change.new_values,
                        "timestamp": change.timestamp
                    }));
                }
                Operation::Delete => {
                    records.push(json!({
                        "action": "delete",
                        "table": change.table_name, 
                        "data": change.old_values,
                        "timestamp": change.timestamp
                    }));
                }
            }
        }
        
        serde_json::to_string(&records)
            .map_err(|e| DestinationError::SerializationError(e.to_string()))
    }
}
```

### Step 4: Add Error Handling and Retries

Implement robust error handling:

```rust
impl MyCustomDestination {
    async fn send_with_retries(&self, payload: String) -> Result<(), DestinationError> {
        let mut attempts = 0;
        let max_attempts = 3;
        
        while attempts < max_attempts {
            match self.send_to_target(&payload).await {
                Ok(_) => return Ok(()),
                Err(e) if self.is_retryable_error(&e) => {
                    attempts += 1;
                    if attempts < max_attempts {
                        let backoff_ms = 2_u64.pow(attempts) * 1000;
                        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                        continue;
                    }
                }
                Err(e) => return Err(e),
            }
        }
        
        Err(DestinationError::RetryExhausted(format!("Failed after {} attempts", max_attempts)))
    }

    async fn send_to_target(&self, payload: &str) -> Result<(), DestinationError> {
        let client = reqwest::Client::new();
        let response = client
            .post(&self.api_endpoint)
            .header("Authorization", format!("Bearer {}", self.auth_token))
            .header("Content-Type", "application/json")
            .body(payload.to_string())
            .send()
            .await
            .map_err(|e| DestinationError::NetworkError(e.to_string()))?;

        if !response.status().is_success() {
            return Err(DestinationError::HttpError(
                response.status().as_u16(),
                format!("Request failed: {}", response.text().await.unwrap_or_default())
            ));
        }

        Ok(())
    }

    fn is_retryable_error(&self, error: &DestinationError) -> bool {
        match error {
            DestinationError::NetworkError(_) => true,
            DestinationError::HttpError(status, _) => {
                // Retry on 5xx server errors and some 4xx errors
                *status >= 500 || *status == 429
            }
            _ => false,
        }
    }
}
```

### Step 5: Use Your Custom Destination

In your main application:

```rust
use etl::pipeline::Pipeline;
use etl::store::both::memory::MemoryStore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = MemoryStore::new();
    let destination = MyCustomDestination::new(
        "https://api.example.com/ingest".to_string(),
        "your-auth-token".to_string()
    );
    
    let mut pipeline = Pipeline::new(pipeline_config, store, destination);
    pipeline.start().await?;
    
    Ok(())
}
```

## Validation

Test your custom destination:

1. **Unit tests** for data conversion logic
2. **Integration tests** with a test target system
3. **Error simulation** to verify retry behavior
4. **Load testing** with realistic data volumes

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_data_conversion() {
        let destination = MyCustomDestination::new(
            "http://test".to_string(),
            "token".to_string()
        );
        
        // Create test batch
        let batch = create_test_batch();
        
        // Test conversion
        let result = destination.convert_batch_to_target_format(&batch);
        assert!(result.is_ok());
        
        // Verify JSON structure
        let json: serde_json::Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert!(json.is_array());
    }
}
```

## Troubleshooting

**Data not appearing in target system:**
- Enable debug logging to see conversion output
- Check target system's ingestion logs
- Verify authentication credentials

**High error rates:**
- Review retry logic and backoff timing
- Check if target system has rate limits
- Consider implementing circuit breaker pattern

**Performance issues:**
- Profile data conversion logic
- Consider batch size tuning
- Implement connection pooling for database destinations

## Rollback

If your destination isn't working:
1. Switch back to [`MemoryDestination`](../../reference/memory-destination/) for testing
2. Check ETL logs for specific error messages
3. Test destination logic in isolation

## Next Steps

- **Add monitoring** → [Performance monitoring](performance/)
- **Handle schema changes** → [Schema change handling](schema-changes/) 
- **Production deployment** → [Debugging guide](debugging/)

## See Also

- [Destination API Reference](../../reference/destination-trait/) - Complete trait documentation
- [BigQuery destination example](https://github.com/supabase/etl/blob/main/etl-destinations/src/bigquery/) - Real-world implementation
- [Error handling patterns](../../explanation/error-handling/) - Best practices for error management