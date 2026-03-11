use std::sync::Once;

/// Ensures crypto provider is only initialized once.
static INIT_CRYPTO: Once = Once::new();

/// Installs the default cryptographic provider for rustls.
pub fn init() {
    INIT_CRYPTO.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("failed to install default crypto provider");
    });
}
