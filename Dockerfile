# syntax=docker/dockerfile:1

ARG RUST_VERSION=nightly-bookworm-slim
FROM rustlang/rust:${RUST_VERSION} AS build

ENV UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    mqtt2postgres

WORKDIR /app

RUN --mount=type=bind,source=src,target=src \
    --mount=type=bind,source=Cargo.toml,target=Cargo.toml \
    --mount=type=bind,source=Cargo.lock,target=Cargo.lock \
    --mount=type=cache,target=/app/target/ \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
    <<EOF
set -e
cargo build --locked --release
cp ./target/release/mqtt2postgres /bin/mqtt2postgres
EOF

FROM gcr.io/distroless/cc-debian12

COPY --from=build /etc/passwd /etc/passwd
COPY --from=build /etc/group /etc/group
COPY --from=build /bin/mqtt2postgres /bin/

USER mqtt2postgres

CMD ["/bin/mqtt2postgres"]