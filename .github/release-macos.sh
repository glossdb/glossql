#!/bin/sh
# The macOS half of a release: Apple Silicon only (ruled — Intel Macs
# are not a target), built from a laptop rather than a runner. Uploads
# the tarball to the tagged GitHub release and rewrites the tap formula
# with the artifact's checksum — copy that file to
# glossdb/homebrew-glossql/Formula/glossql.rb and push the tap.
set -eu
cd "$(dirname "$0")/.."

version=$(sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1)
tag="v${version}"
artifact="glossql-${version}-aarch64-apple-darwin.tar.gz"

cargo build --release -j8 -p glossql-serverd --features embed-weights
tar -C target/release -czf "target/${artifact}" glossql
sha=$(shasum -a 256 "target/${artifact}" | cut -d' ' -f1)

gh release view "$tag" >/dev/null 2>&1 \
  || gh release create "$tag" --draft --title "$tag" --notes ""
gh release upload "$tag" "target/${artifact}" --clobber

sed -e "s/{{version}}/${version}/g" -e "s/{{sha256}}/${sha}/g" \
  .github/homebrew/glossql.rb.tmpl > .github/homebrew/glossql.rb

echo "uploaded ${artifact} (sha256 ${sha})"
echo "formula at .github/homebrew/glossql.rb — copy to glossdb/homebrew-glossql/Formula/ and push"
