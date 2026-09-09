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
    && useradd --create-home --uid 1000 glossql
COPY --from=build /src/target/release/glossql /usr/local/bin/glossql
USER glossql
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
# the disk, the rest to the writable layer. A different box overrides
# the command.
CMD ["glossql", "--addr", "0.0.0.0:8080", \
     "--memory-limit", "4096", "--cube-cache", "2048", "--spill-limit", "6144"]
