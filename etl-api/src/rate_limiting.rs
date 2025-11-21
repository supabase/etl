use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use actix_web::{
    dev::{Service, ServiceRequest, ServiceResponse, Transform},
    Error, HttpResponse,
    http::StatusCode,
};
use dashmap::DashMap;
use futures::future::{ok, Ready, LocalBoxFuture};
use futures::task::{Context, Poll};
use tracing::warn;

/// Rate limiter configuration
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Maximum number of requests per time window
    pub max_requests: u32,
    /// Time window duration
    pub window_duration: Duration,
    /// Maximum failed authentication attempts
    pub max_auth_failures: u32,
    /// Time window for authentication failures
    pub auth_failure_window: Duration,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            max_requests: 100,                     // 100 requests
            window_duration: Duration::from_secs(60), // per minute
            max_auth_failures: 5,                  // 5 failed auth attempts
            auth_failure_window: Duration::from_secs(300), // in 5 minutes
        }
    }
}

/// Tracks request counts and timestamps for rate limiting
#[derive(Debug, Clone)]
struct RateLimitEntry {
    count: u32,
    window_start: Instant,
}

/// Tracks authentication failures for IPs
#[derive(Debug, Clone)]
struct AuthFailureEntry {
    count: u32,
    window_start: Instant,
}

/// Rate limiter that tracks requests per IP address
pub struct RateLimiter {
    config: RateLimiterConfig,
    requests: Arc<DashMap<IpAddr, RateLimitEntry>>,
    auth_failures: Arc<DashMap<IpAddr, AuthFailureEntry>>,
}

impl RateLimiter {
    pub fn new(config: RateLimiterConfig) -> Self {
        Self {
            config,
            requests: Arc::new(DashMap::new()),
            auth_failures: Arc::new(DashMap::new()),
        }
    }

    /// Check if an IP is rate limited
    pub fn is_rate_limited(&self, ip: &IpAddr) -> bool {
        let now = Instant::now();

        // Check general rate limiting
        if let Some(mut entry) = self.requests.get_mut(ip) {
            // Reset window if expired
            if now.duration_since(entry.window_start) >= self.config.window_duration {
                entry.count = 0;
                entry.window_start = now;
            }

            if entry.count >= self.config.max_requests {
                return true;
            }
        }

        // Check authentication failure blocking
        if let Some(mut entry) = self.auth_failures.get_mut(ip) {
            // Reset window if expired
            if now.duration_since(entry.window_start) >= self.config.auth_failure_window {
                entry.count = 0;
                entry.window_start = now;
            }

            if entry.count >= self.config.max_auth_failures {
                return true;
            }
        }

        false
    }

    /// Record a request from an IP
    pub fn record_request(&self, ip: &IpAddr) {
        let now = Instant::now();

        self.requests
            .entry(*ip)
            .and_modify(|entry| {
                // Reset window if expired
                if now.duration_since(entry.window_start) >= self.config.window_duration {
                    entry.count = 1;
                    entry.window_start = now;
                } else {
                    entry.count += 1;
                }
            })
            .or_insert(RateLimitEntry {
                count: 1,
                window_start: now,
            });
    }

    /// Record an authentication failure
    pub fn record_auth_failure(&self, ip: &IpAddr) {
        let now = Instant::now();

        self.auth_failures
            .entry(*ip)
            .and_modify(|entry| {
                // Reset window if expired
                if now.duration_since(entry.window_start) >= self.config.auth_failure_window {
                    entry.count = 1;
                    entry.window_start = now;
                } else {
                    entry.count += 1;
                }
            })
            .or_insert(AuthFailureEntry {
                count: 1,
                window_start: now,
            });
    }

    /// Clean up expired entries to prevent memory leaks
    pub fn cleanup_expired(&self) {
        let now = Instant::now();

        // Clean up request entries
        self.requests.retain(|_, entry| {
            now.duration_since(entry.window_start) < self.config.window_duration
        });

        // Clean up auth failure entries
        self.auth_failures.retain(|_, entry| {
            now.duration_since(entry.window_start) < self.config.auth_failure_window
        });
    }
}

/// Rate limiting middleware
pub struct RateLimitingMiddleware {
    rate_limiter: Arc<RateLimiter>,
}

impl RateLimitingMiddleware {
    pub fn new(rate_limiter: Arc<RateLimiter>) -> Self {
        Self { rate_limiter }
    }
}

impl<S, B> Transform<S, ServiceRequest> for RateLimitingMiddleware
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = RateLimitingMiddlewareService<S>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ok(RateLimitingMiddlewareService {
            service,
            rate_limiter: self.rate_limiter.clone(),
        })
    }
}

pub struct RateLimitingMiddlewareService<S> {
    service: S,
    rate_limiter: Arc<RateLimiter>,
}

impl<S, B> Service<ServiceRequest> for RateLimitingMiddlewareService<S>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    S::Future: 'static,
    B: 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(&self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&self, req: ServiceRequest) -> Self::Future {
        // Extract client IP address
        let client_ip = req
            .connection_info()
            .peer_addr()
            .and_then(|addr| addr.split(':').next())
            .and_then(|ip| ip.parse().ok())
            .unwrap_or(IpAddr::from([127, 0, 0, 1])); // fallback to localhost

        // Check rate limiting
        if self.rate_limiter.is_rate_limited(&client_ip) {
            warn!("Rate limit exceeded for IP: {}", client_ip);
            
            let (http_req, _) = req.into_parts();
            let response = HttpResponse::TooManyRequests()
                .insert_header(("Retry-After", "60"))
                .json(serde_json::json!({
                    "error": "Rate limit exceeded. Please try again later."
                }));

            return Box::pin(async move {
                Ok(ServiceResponse::new(http_req, response))
            });
        }

        // Record the request
        self.rate_limiter.record_request(&client_ip);

        // Continue with the request
        let fut = self.service.call(req);
        Box::pin(async move {
            let res = fut.await?;
            Ok(res)
        })
    }
}
