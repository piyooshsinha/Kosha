# Kosha container image.
#
#   docker build -t kosha .
#   docker run -d -p 9000:9000 -v kosha-data:/data \
#     -e KOSHA_ACCESS_KEY=admin -e KOSHA_SECRET_KEY=change-me-please kosha
#
# Any `kosha --help` setting can be passed as a KOSHA_* environment variable.

FROM node:22-bookworm-slim AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build

FROM rust:1-bookworm AS build
WORKDIR /src
COPY . .
COPY --from=ui /src/ui/dist ui/dist
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p kosha-server \
    && cp target/release/kosha /usr/local/bin/kosha

FROM debian:bookworm-slim
# CA roots for HTTPS cold tiers and webhooks, taken from the build image.
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
RUN useradd --system --uid 10001 --home-dir /data kosha \
    && install -d -o kosha -g kosha /data
COPY --from=build /usr/local/bin/kosha /usr/local/bin/kosha
USER kosha
ENV KOSHA_LISTEN=0.0.0.0:9000 KOSHA_DATA_DIR=/data
VOLUME /data
EXPOSE 9000
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9000"]
ENTRYPOINT ["/usr/local/bin/kosha"]
