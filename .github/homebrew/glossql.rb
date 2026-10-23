# The tap formula, rewritten by .github/release-macos.sh at each
# release; lives at glossdb/homebrew-glossql/Formula/glossql.rb.
class Glossql < Formula
  desc "Context language server: SQL-shaped surface over a workspace's data and context"
  homepage "https://github.com/glossdb/glossql"
  url "https://github.com/glossdb/glossql/releases/download/v0.3.0/glossql-0.3.0-aarch64-apple-darwin.tar.gz"
  sha256 "81ab487b1b1c1282dbe8228d05854b72f6637d24412370e0e9dad4f474601a61"
  version "0.3.0"
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
