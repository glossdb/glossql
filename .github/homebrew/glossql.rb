# The tap formula, rewritten by .github/release-macos.sh at each
# release; lives at glossdb/homebrew-glossql/Formula/glossql.rb.
class Glossql < Formula
  desc "Context language server: SQL-shaped surface over a workspace's data and context"
  homepage "https://github.com/glossdb/glossql"
  url "https://github.com/glossdb/glossql/releases/download/v0.1.6/glossql-0.1.6-aarch64-apple-darwin.tar.gz"
  sha256 "aff9eea548f9cc74e5c8585ddc1f7c88f9fb65c03c35069aea1bfdca6ea277cd"
  version "0.1.6"
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
