---
name: glossql-release
description: How a glossql release is cut — the order of operations across both repos, the tag mechanics, and the sharp edges that have already cost a failed run. Use before tagging, changing the release workflow, or touching packaging.
---

# Releasing glossql

One release is one version everywhere: `version` in the workspace
`Cargo.toml` (`[workspace.package]`) is the single bump point, and
`glossql --version` answers with it. The artifacts: a macOS arm64
tarball behind the brew tap, and two x86_64 debs (cpu and cuda).
**Never built**: macOS x86_64, ARM Linux, Jetson — ruled, don't
propose them back.

The detail lives in the files themselves — `.github/workflows/
release.yml`, `.github/release-macos.sh`, `docs/start/install.md`,
the deb tables in `crates/serverd/Cargo.toml`. What follows is the
order, which is written nowhere else.

## The order

1. If tabicl-candle changed, push it first — the workflow checks out
   its GitHub main, not the sibling directory.
2. Bump `version` in the workspace `Cargo.toml`. Workspace
   `cargo test` green — a datafusion pin move since the last release
   fails the vendored-guide test until `vendor/datafusion/refresh.sh
   <tag>` has run.
3. Tag and push: `git tag v<version> && git push origin v<version>`.
   The workflow reconverts the weights from the public checkpoints
   (digest-checked), builds both debs, and uploads them to a draft
   release.
4. The laptop half: `.github/release-macos.sh` builds with
   `--features embed-weights`, uploads the tarball, and renders
   `.github/homebrew/glossql.rb` with the real checksum.
5. Copy that rendered file to
   `glossdb/homebrew-glossql/Formula/glossql.rb` and push the tap.
6. When all three assets sit on the draft:
   `gh release edit v<version> --draft=false`. Publish once,
   complete — that is the standing preference.

## Sharp edges (each one has already fired)

- A tag-push run uses the workflow file **at the tag's commit**. A
  workflow fix after tagging needs the tag re-pointed:
  `git tag -f v<version> && git push -f origin v<version>`.
  `workflow_dispatch` builds but never uploads (the upload step is
  gated on a tag ref).
- Draft assets are not served at the public download URL — brew
  fails against a draft with a bare "Download failed". Publishing is
  the fix, not the formula.
- `CUDA_COMPUTE_CAP=80`, not lower: candle's `compatibility.cuh`
  defines `__hmax_nan` below arch 800 and CUDA ≥ 12.2's own
  `cuda_fp16.hpp` defines it too — a 7.5 build fails in nvcc.
  The stated floor is Ampere everywhere the flavor is described.
- An `embed-weights` build refuses to proceed without converted
  weights in the sibling checkout — that is deliberate; the release
  artifact must never silently lack them.
- Never put a formula in the tap before the tarball's sha256 exists;
  a placeholder breaks `brew install` publicly.
