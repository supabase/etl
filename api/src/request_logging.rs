use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use actix_web::{
    Error,
    body::MessageBody,
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
};
use tracing::{Instrument, info, warn};
use uuid::Uuid;

/// Custom request logging middleware that logs request start and completion events
pub struct RequestLogging;

impl<S, B> Transform<S, ServiceRequest> for RequestLogging
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = RequestLoggingMiddleware<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(RequestLoggingMiddleware { service }))
    }
}

pub struct RequestLoggingMiddleware<S> {
    service: S,
}

impl<S, B> Service<ServiceRequest> for RequestLoggingMiddleware<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: MessageBody + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let start_time = Instant::now();

        // Extract project/tenant_id for the span
        let project = req
            .headers()
            .get("tenant_id")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        // Extract additional context for logging
        let content_length = req
            .headers()
            .get("content-length")
            .and_then(|h| h.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        let user_agent = req
            .headers()
            .get("user-agent")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        let content_type = req
            .headers()
            .get("content-type")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        let remote_addr = req
            .connection_info()
            .peer_addr()
            .unwrap_or("unknown")
            .to_string();

        let host = req
            .headers()
            .get("host")
            .and_then(|h| h.to_str().ok())
            .unwrap_or("unknown")
            .to_string();

        // Extract method, uri, etc. before moving req into the span
        let method = req.method().to_string();
        let uri = req.uri().to_string();
        let path = req.path().to_string();
        let query_string = req.query_string().to_string();
        let version = format!("{:?}", req.version());

        // Create a custom span with all the request details
        let request_id = Uuid::new_v4().to_string();
        let span = tracing::info_span!(
            "HTTP request",
            method = %method,
            uri = %uri,
            path = %path,
            query_string = %query_string,
            version = %version,
            project = %project,
            content_length = content_length,
            user_agent = %user_agent,
            remote_addr = %remote_addr,
            host = %host,
            content_type = %content_type,
            request_id = %request_id,
        );

        // Enter span and log request start.
        {
            let _enter = span.enter();
            info!("HTTP request received");
        }

        let fut = self.service.call(req);

        Box::pin(
            async move {
                let res = fut.await;
                let duration = start_time.elapsed();

                match res {
                    Ok(response) => {
                        let status_code = response.status().as_u16();
                        let response_size = response
                            .headers()
                            .get("content-length")
                            .and_then(|h| h.to_str().ok())
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(0);

                        let response_content_type = response
                            .headers()
                            .get("content-type")
                            .and_then(|h| h.to_str().ok())
                            .unwrap_or("unknown");

                        // Log successful request completion
                        info!(
                            status_code = status_code,
                            response_size = response_size,
                            response_content_type = %response_content_type,
                            duration_ms = duration.as_millis(),
                            duration_us = duration.as_micros(),
                            "HTTP request completed successfully"
                        );

                        Ok(response)
                    }
                    Err(error) => {
                        // Log error request completion
                        warn!(
                            error = %error,
                            error_type = %std::any::type_name_of_val(&error),
                            duration_ms = duration.as_millis(),
                            duration_us = duration.as_micros(),
                            "HTTP request completed with error"
                        );

                        Err(error)
                    }
                }
            }
            .instrument(span),
        )
    }
}
