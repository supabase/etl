/// Awaits a future with future lifecycle instrumentation when enabled.
#[macro_export]
macro_rules! profile_future {
    ($label:literal, $future:expr) => {{
        #[cfg(feature = "hotpath")]
        {
            hotpath::future!($future, label = $label).await
        }
        #[cfg(not(feature = "hotpath"))]
        {
            $future.await
        }
    }};
}

pub mod common;
pub mod table_copy;
pub mod table_streaming;
