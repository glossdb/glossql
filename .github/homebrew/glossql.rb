# The tap formula, rewritten by .github/release-macos.sh at each
# release; lives at glossdb/homebrew-glossql/Formula/glossql.rb.
class Glossql < Formula
  desc "Context language server: SQL-shaped surface over a workspace's data and context"
  homepage "https://github.com/glossdb/glossql"
  url "https://github.com/glossdb/glossql/releases/download/v0.1.5/glossql-0.1.5-aarch64-apple-darwin.tar.gz"
  sha256 "36966ff2c5685dec6505e2969b0e8f916f86e8bbf3f58f9bfe689fe7ab29fb8e"
  version "0.1.5"
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
