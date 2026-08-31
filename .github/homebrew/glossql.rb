# The tap formula, rewritten by .github/release-macos.sh at each
# release; lives at glossdb/homebrew-glossql/Formula/glossql.rb.
class Glossql < Formula
  desc "Context language server: SQL-shaped surface over a workspace's data and context"
  homepage "https://github.com/glossdb/glossql"
  url "https://github.com/glossdb/glossql/releases/download/v0.1.0/glossql-0.1.0-aarch64-apple-darwin.tar.gz"
  sha256 "3a4b31e79abc42351decf411fc180e69a2a552f6c925dee72735c04a7d3dc59b"
  version "0.1.0"
  license "Apache-2.0"

  depends_on :macos
  depends_on arch: :arm64

  def install
    bin.install "serverd"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/serverd --version")
  end
end
