# Shopping Sessions: native Rust pipeline (rust-fused).
# Build stage: pinned toolchain; crates pinned by Cargo.lock (network available at build).
FROM rust:1.85@sha256:e51d0265072d2d9d5d320f6a44dde6b9ef13653b035098febd68cce8fa7c0bc4 AS build
# x86-64-v3 (AVX2) runs on the scorer (Cascade Lake+) and on hosts without AVX-512.
ARG TARGET_CPU=x86-64-v3
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
# PGO: pgo/sessions.profdata (made by tools/dbx_pgo.sh on 20M seed 7) is used when present.
# A profile only steers optimization (inlining, layout); results are identical without it.
COPY pgo ./pgo
RUN PGO=""; if [ -f pgo/sessions.profdata ]; then PGO="-Cprofile-use=/src/pgo/sessions.profdata"; fi; \
    RUSTFLAGS="-C target-cpu=${TARGET_CPU} ${PGO}" cargo build --release --locked \
 && strip target/release/sessions

FROM debian:bookworm-slim@sha256:734a72b3d6142a71993b4bc580ca01d8ca687f77a2c422d96a961d3376b179bc
RUN useradd --uid 1000 --no-create-home --home-dir /scratch app
COPY --from=build /src/target/release/sessions /app/sessions
USER 1000:1000
ENV DATA_DIR=/data OUTPUT_DIR=/output
CMD ["/app/sessions"]
