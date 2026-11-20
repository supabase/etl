#!/usr/bin/env bash
set -euo pipefail
lab_dir=${1:?provide an absolute directory for disposable lab certificates}
case "$lab_dir" in /*) ;; *) exit 1 ;; esac
mkdir -p "$lab_dir/certs"
if test -e "$lab_dir/certs/server.key"; then
  echo "Refusing to replace existing lab certificates" >&2
  exit 1
fi
umask 077
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj '/CN=ETL disposable network test CA' \
  -keyout "$lab_dir/certs/root.key" -out "$lab_dir/certs/root.crt"
openssl req -newkey rsa:2048 -nodes -subj '/CN=source.test' \
  -keyout "$lab_dir/certs/server.key" -out "$lab_dir/certs/server.csr"
cat > "$lab_dir/certs/server.ext" <<'EOF'
subjectAltName=DNS:source.test,DNS:admin.test
extendedKeyUsage=serverAuth
EOF
openssl x509 -req -days 2 -sha256 -in "$lab_dir/certs/server.csr" \
  -CA "$lab_dir/certs/root.crt" -CAkey "$lab_dir/certs/root.key" -CAcreateserial \
  -extfile "$lab_dir/certs/server.ext" -out "$lab_dir/certs/server.crt"
openssl verify -CAfile "$lab_dir/certs/root.crt" "$lab_dir/certs/server.crt"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj '/CN=Untrusted disposable test CA' \
  -keyout "$lab_dir/certs/untrusted.key" -out "$lab_dir/certs/untrusted.crt"
