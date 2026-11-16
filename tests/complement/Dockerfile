# Complement conformance image for asaroton.
# Built from the repository root:  docker build -t complement-asaroton -f tests/complement/Dockerfile .
#
# Complement contract (see complement/README.md "Image requirements"):
#  - EXPOSE 8008 (client, plain HTTP) and 8448 (federation, HTTPS)
#  - SERVER_NAME env var at runtime; CMD may run multiple times
#  - trust the CA at /complement/ca/ca.crt and present a cert signed by /complement/ca/ca.key
#  - HEALTHCHECK until GET /_matrix/client/versions returns 200
#  - registration shared secret "complement" for /_synapse/admin/v1/register (optional)

FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
# A copy taken right after linking can come out with zeroes where the binary's data should be
# (the kernel still reports the unwritten pages as a hole and cp skips them); the server then
# dies at its first random number. Flush the file before copying it and compare the copy.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release -p asaroton \
    && sync target/release/asaroton \
    && cp target/release/asaroton /asaroton \
    && cmp target/release/asaroton /asaroton

FROM debian:bookworm-slim
# scripts/complement.sh removes the untagged images this label marks.
LABEL org.asaroton.complement=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl openssl postgresql postgresql-contrib \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /asaroton /usr/local/bin/asaroton
COPY tests/complement/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENV SERVER_NAME=localhost
EXPOSE 8008 8448
HEALTHCHECK --interval=1s --timeout=1s --start-period=1s --retries=60 \
  CMD curl -fsS http://localhost:8008/_matrix/client/versions || exit 1
ENTRYPOINT ["/entrypoint.sh"]
