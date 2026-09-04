# The tap formula, rewritten by .github/release-macos.sh at each
# release; lives at glossdb/homebrew-glossql/Formula/glossql.rb.
class Glossql < Formula
  desc "Context language server: SQL-shaped surface over a workspace's data and context"
  homepage "https://github.com/glossdb/glossql"
  url "https://github.com/glossdb/glossql/releases/download/v0.1.2/glossql-0.1.2-aarch64-apple-darwin.tar.gz"
  sha256 "8f280eee5291d367aa58e54c7b4d17c1b64a047f1ddc00a2343f5bde99d2e626"
  version "0.1.2"
  license "Apache-2.0"

  depends_on :macos
  depends_on arch: :arm64

  def install
    bin.install "glossql"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/glossql --version")
  end
end
