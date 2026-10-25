# Release image for tessera. Build from the repository root:
#   docker build -t tessera .
# The binary links rustls and sqlx (pure Rust), so the runtime image only needs CA roots.

FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release -p tessera && cp target/release/tessera /tessera

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /tessera /usr/local/bin/tessera
# Signing keys and media live here; mount a volume.
VOLUME ["/data"]
EXPOSE 8008 8448
ENTRYPOINT ["/usr/local/bin/tessera"]
CMD ["--config", "/data/tessera.toml", "serve"]
