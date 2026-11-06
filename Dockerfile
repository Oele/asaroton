# Release image for asaroton. Build from the repository root:
#   docker build -t asaroton .
# The binary links rustls and sqlx (pure Rust), so the runtime image only needs CA roots.

FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p asaroton && cp target/release/asaroton /asaroton

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /asaroton /usr/local/bin/asaroton
# Signing keys and media live here; mount a volume.
VOLUME ["/data"]
EXPOSE 8008 8448
ENTRYPOINT ["/usr/local/bin/asaroton"]
CMD ["--config", "/data/asaroton.toml", "serve"]
