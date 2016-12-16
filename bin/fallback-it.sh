#!/usr/bin/env bash
#

# defaultSpecs () { echo '*.ReadFilesSpec *.QueryFilesSpec *.WriteFilesSpec *.ManageFilesSpec'; }
defaultSpecs () { echo '*.ResultFileQueryRegressionSpec'; }
run () { echo >&2 "$@" && "$@"; }

: ${TARGET=it/testOnly}
: ${TESTS=$(defaultSpecs)}
: ${CHROOT=it/src/main/resources/tests/zips.data}

[[ -n "$TRACE" ]] && TRACE="-Dygg.trace=$TRACE"

makeJson () { printf '{"fallback":{"connectionUri":"$s"}}\n' "$CHROOT"; }

QUASAR_FALLBACK="$(makeJson)" run ./sbt $TRACE "$TARGET $TESTS" |& \
  seq-map truncate -dm 1000
