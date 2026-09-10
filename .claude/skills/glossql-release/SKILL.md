---
name: glossql-release
description: How a glossql release is cut — the order of operations across both repos, the tag mechanics, and the sharp edges that have already cost a failed run. Use before tagging, changing the release workflow, or touching packaging.
---

# Releasing glossql

One release is one version everywhere: `version` in the workspace
`Cargo.toml` (`[workspace.package]`) is the single bump point, and
`glossql --version` answers with it. The artifacts: a macOS arm64
tarball behind the brew tap, one x86_64 deb, and the server image on
GHCR (`ghcr.io/glossdb/glossql:<version>` and `:latest`, built from
the `Dockerfile` at the tag). No model rides any of them: the band
model is the glosskernels service, released from its own repository
on its own cadence. **Never built**: macOS x86_64,
ARM Linux, Jetson, a cuda flavor — ruled, don't propose them back.

The detail lives in the files themselves — `.github/workflows/
release.yml`, `.github/release-macos.sh`, `docs/start/install.md`,
the deb tables in `crates/serverd/Cargo.toml`. What follows is the
order, which is written nowhere else.

## The order

1. If the kernel service's wire changed, glosskernels releases first
   — the server's live test against a running service is the check.
2. Bump `version` in the workspace `Cargo.toml`. Workspace
   `cargo test` green — a datafusion pin move since the last release
   fails the vendored-guide test until `vendor/datafusion/refresh.sh
   <tag>` has run.
3. Tag and push: `git tag v<version> && git push origin v<version>`.
   The workflow builds the deb and uploads it to a draft release, and
   builds the image and pushes it to GHCR — the image is public the
   moment the push lands, the deb only when the draft is published.
4. The laptop half: `.github/release-macos.sh` builds, uploads the
   tarball, and renders `.github/homebrew/glossql.rb` with the real
   checksum.
5. Copy that rendered file to
   `glossdb/homebrew-glossql/Formula/glossql.rb` and push the tap.
6. When both assets sit on the draft:
   `gh release edit v<version> --draft=false`. Publish once,
   complete — that is the standing preference.

## Sharp edges (each one has already fired)

- A tag-push run uses the workflow file **at the tag's commit**. A
  workflow fix after tagging needs the tag re-pointed:
  `git tag -f v<version> && git push -f origin v<version>`.
  `workflow_dispatch` builds the deb without uploading it (the upload
  step is gated on a tag ref) and pushes the image as `:main` and
  `:sha-<commit>` — the road to an image between releases; `:latest`
  moves at a tag only.
- Draft assets are not served at the public download URL — brew
  fails against a draft with a bare "Download failed". Publishing is
  the fix, not the formula.
- Never put a formula in the tap before the tarball's sha256 exists;
  a placeholder breaks `brew install` publicly.
- A package's first push to GHCR lands **private** (packages do not
  inherit the repository's visibility); the package is made public
  once, in the org's package settings, and cannot be made private
  again. Until then `docker pull` answers "denied".
