# Channel-binding certificate fixtures

These synthetic, self-signed certificates use `CN=example.com`, serial 1, 2048-bit RSA or P-384 ECDSA keys, and the signature hash in each filename. They are parser/hash inputs, not trusted certificates for TLS handshakes; their validity dates do not affect the tests. Private keys were discarded.

Generate replacements using temporary keys and OpenSSL:

```sh
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out rsa.key
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-384 -out ecdsa.key
openssl req -new -x509 -key rsa.key -sha256 -days 3650 \
  -subj /CN=example.com -set_serial 1 -outform DER -out rsa-sha256.der
openssl dgst -sha256 -binary rsa-sha256.der | od -An -tx1
```

Repeat the certificate command with the key and signature hash from each filename. Update the fixed expected digests in `tls.rs` when regenerating: hash the complete DER with the signature hash, except SHA-1 certificates use SHA-256 as required by RFC 5929 section 4.1. Never commit the temporary keys.
