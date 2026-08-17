#!/usr/bin/env bash
# Re-capture into a temp dir and diff against the committed goldens.
# Any output means behaviour moved. Pass corpus names to narrow:
#   ./goldens/diff.sh              # all four (~10 min, rel-event is 8 of it)
#   ./goldens/diff.sh fin rel-f1   # the fast ones
#   UPDATE=1 ./goldens/diff.sh     # accept the new capture as the baseline
set -u
cd "$(dirname "$0")/.."
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
RUN="cargo run --release -q -p glossql-serverd --example capture_goldens --"
E=../dataraum-eval/corpora/relbench

cargo build --release -q -p glossql-serverd --example capture_goldens || exit 1
for c in ${@:-fin rel-f1 rel-event booksql}; do
  case $c in
    fin)       $RUN ~/glossql-ws fin "$TMP/fin" --existing \
                 --setup=goldens/fin/setup.glossql ;;
    rel-f1)    $RUN $E/rel-f1/tables f1 "$TMP/rel-f1" ;;
    rel-event) $RUN $E/rel-event/tables evt "$TMP/rel-event" ;;
    booksql)   $RUN ../testdata/booksql/Tables books "$TMP/booksql" \
                 --setup=goldens/booksql/setup.glossql ;;
    *) echo "no corpus $c" >&2; exit 1 ;;
  esac
  if [ "${UPDATE:-}" = 1 ]; then
    cp "$TMP/$c"/goldens/*.json "goldens/$c/" && echo "$c: updated"
  else
    diff -r -x '*.glossql' "goldens/$c" "$TMP/$c/goldens" && echo "$c: same"
  fi
done
