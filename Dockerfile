# The server image: one Rust binary on a distroless base — no model, no
# weights, no GPU; the commodity container (docs/start/install.md, "The
# container"). Built at a release tag by the workflow and pushed to
# ghcr.io/glossdb/glossql; `docker build .` builds the same image from
# any checkout. The build context is what the binary compiles and
# embeds (.dockerignore).
# Both bases by digest, moved by Dependabot; the rust base carries the
# toolchain rust-toolchain.toml names. The registry and the target are
# cache mounts, so a rebuild compiles the workspace alone.
FROM rust:1.98-trixie@sha256:a8a5f0a1e5fe7dfe1d352591e4a1c7dd2c08fd70475cae872cf3458ba0df0546 AS build
WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p glossql-serverd \
    && cp target/release/glossql /glossql

# What the binary links is glibc, libm and libgcc (`ldd` on it), and
# what it needs beside them is the root certificates — the issuer's
# keys, glossthesi, Postgres over TLS and the object stores are
# all reached over https — and a /tmp for the engine's spill files.
# The distroless C-runtime image is exactly that, with an unprivileged
# user and no shell or package manager.
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:3ee00291cf386ec52a552e6a858d555debd74776eb4bfa53fa3df2cd6677515e
COPY --from=build /glossql /usr/local/bin/glossql
USER nonroot
EXPOSE 8080
# No workspace, no directory, no file: the state is the catalog and the
# warehouse the environment names, and the environment is what the
# platform injects — secrets included. Without both the server refuses
# to start, naming them. The image holds no value of its own.
#
# The sizes for a box with 8 GiB of memory and an 8 GiB ephemeral disk,
# two numbers set from two facts: the engine's pool and the cube cache
# at their defaults, 6 GiB tracked, the rest of the memory to the
# process and what the pool does not track; the spill bound 6 GiB of
# the disk, the rest to the writable layer. They are variables, so a
# different box injects its own numbers beside the rest of its
# environment.
ENV GLOSSQL_ADDR=0.0.0.0:8080 \
    GLOSSQL_MEMORY_LIMIT=4096 \
    GLOSSQL_CUBE_CACHE=2048 \
    GLOSSQL_SPILL_LIMIT=6144
CMD ["/usr/local/bin/glossql"]
