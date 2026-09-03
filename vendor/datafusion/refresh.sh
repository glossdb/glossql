#!/bin/sh
# Refresh the vendored DataFusion SQL guide at a tag — the pages the
# door serves as `doc://vendor/datafusion/sql/…`. Run at every datafusion
# pin move; the serverd suite refuses a VERSION that is not Cargo.lock's.
#
#   vendor/datafusion/refresh.sh 54.1.0
#
# DDL, DML, COPY options, information_schema, prepared statements and
# EXPLAIN are not fetched: the door closes the first four and serves
# nothing the last two describe.
set -eu
tag="${1:?usage: refresh.sh <datafusion tag, e.g. 54.1.0>}"
here="$(cd "$(dirname "$0")" && pwd)"
upstream="docs/source/user-guide/sql"
fetch() {
  gh api "repos/apache/datafusion/contents/$upstream/$1.md?ref=$tag" --jq '.content' | base64 -d
}
rm -rf "$here/sql"
mkdir -p "$here/sql/scalar"
for page in select subqueries window_functions aggregate_functions operators data_types special_functions; do
  fetch "$page" > "$here/sql/$page.md"
done
# scalar_functions.md is 160 KB: one page per `## ` section, the license
# block repeated on each, the section's name as the page title.
fetch scalar_functions > "$here/scalar_functions.tmp"
awk -v dir="$here/sql/scalar" '
  /^<!---/ { inlic = 1 }
  inlic { lic = lic $0 "\n"; if ($0 ~ /^-->/) inlic = 0; next }
  /^## / {
    if (out) close(out)
    name = substr($0, 4); sub(/ Functions$/, "", name)
    slug = tolower(name); gsub(/ /, "_", slug)
    out = dir "/" slug ".md"
    printf "%s\n# Scalar functions: %s\n\n", lic, tolower(name) > out
    next
  }
  out { print > out }
' "$here/scalar_functions.tmp"
rm "$here/scalar_functions.tmp"
printf '%s\n' "$tag" > "$here/VERSION"
