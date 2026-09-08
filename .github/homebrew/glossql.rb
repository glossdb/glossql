# The tap formula, rewritten by .github/release-macos.sh at each
# release; lives at glossdb/homebrew-glossql/Formula/glossql.rb.
class Glossql < Formula
  desc "Context language server: SQL-shaped surface over a workspace's data and context"
  homepage "https://github.com/glossdb/glossql"
  url "https://github.com/glossdb/glossql/releases/download/v0.1.3/glossql-0.1.3-aarch64-apple-darwin.tar.gz"
  sha256 "58c4fab86b929192dea3d5f7df2a2da4c111a1d830f2dcb31f9fcfaebd7ae61a"
  version "0.1.3"
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
