# The server image: one Rust binary on a slim Debian — no model, no
# weights, no GPU; the commodity container (docs/start/install.md, "The
# container"). Built at a release tag by the workflow and pushed to
# ghcr.io/glossdb/glossql; `docker build .` builds the same image from
# any checkout. The build context is what the binary compiles and
# embeds (.dockerignore).
FROM rust:1-trixie AS build
WORKDIR /src
COPY . .
RUN cargo build --release -p glossql-serverd

FROM debian:trixie-slim
# The issuer's keys, the kernel service, Postgres over TLS and the
# object stores are all reached over https: the roots have to be here.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 glossql \
    && mkdir /workspace \
    && chown glossql /workspace
COPY --from=build /src/target/release/glossql /usr/local/bin/glossql
USER glossql
# The workspace directory: apps/ alone when the catalog and the
# warehouse are named in the environment, the whole state when they
# are not (mount a directory here). Also the working directory, so an
# `.env` mounted beside apps/ is read.
WORKDIR /workspace
EXPOSE 8080
# The sizes for a box with 8 GiB of memory and an 8 GiB ephemeral disk:
# the engine's pool 3 GiB, so its spill (bounded at twice the pool) fits
# the disk; the cube cache 1 GiB; the rest of the memory is the process
# and what the pool does not track. A larger box overrides the command.
CMD ["glossql", "--workspace", "/workspace", "--addr", "0.0.0.0:8080", \
     "--memory-limit", "3072", "--cube-cache", "1024"]
