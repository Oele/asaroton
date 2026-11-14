# Release image for asaroton. Build from the repository root:
#   docker build -t asaroton .
# The binary links rustls and sqlx (pure Rust), so the runtime image only needs CA roots.

FROM rust:1-bookworm AS builder
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --release -p asaroton && cp target/release/asaroton /asaroton \
    && mkdir -p /licenses/musivum \
    && m=$(dirname "$(find /usr/local/cargo/git/checkouts -maxdepth 3 -name NOTICE -path '*musivum*' | head -n 1)") \
    && cp "$m/LICENSE" "$m/NOTICE" /licenses/musivum/

FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=builder /asaroton /usr/local/bin/asaroton
# The Apache License asks for the licence and the notice to travel with the binary: asaroton's,
# and musivum's, which is built into it (the SFU in the process).
COPY LICENSE NOTICE /usr/share/doc/asaroton/
COPY --from=builder /licenses/musivum/ /usr/share/doc/asaroton/musivum/
# Signing keys and media live here; mount a volume.
VOLUME ["/data"]
EXPOSE 8008 8448
ENTRYPOINT ["/usr/local/bin/asaroton"]
CMD ["--config", "/data/asaroton.toml", "serve"]
