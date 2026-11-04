#!/bin/sh
# The spatial kit on the TLC taxi zones of New York City: a fresh workspace,
# the kit in the opener, the statements of taxi_zones.sql through the CLI door.
#
#   examples/spatial/run.sh /path/to/a/new/workspace
#
# The zone polygons come from the TLC (one download, kept under data/). The
# built extension is loaded by path; a second run on the same directory
# re-declares what stands.
set -eu
REPO=$(cd "$(dirname "$0")/../.." && pwd)
WS=${1:?a workspace directory}
mkdir -p "$WS/ws" "$WS/data"
if [ ! -f "$WS/data/taxi_zones/taxi_zones.shp" ]; then
  curl -sL https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip -o "$WS/data/taxi_zones.zip"
  unzip -oq "$WS/data/taxi_zones.zip" -d "$WS/data"
fi
cat > "$WS/glossql.sql" <<SQL
.startup_text none
LOAD '$REPO/build/release/extension/glossql/glossql.duckdb_extension';
ATTACH 'ducklake:ws.ducklake' AS ws (DATA_PATH 'ws/');
USE ws;
.output /dev/null
PRAGMA glossql;
FROM glossql_kit('$REPO/kits/spatial/kit.sql');
.output
SQL
cd "$WS" && sed "s#{DATA}#$WS/data/taxi_zones#g" "$REPO/examples/spatial/taxi_zones.sql" | duckdb -unsigned -bail -init glossql.sql
