#!/usr/bin/env bash
#

sbtVersion="0.13.11"

# -Dprecog.optimize
# -Dprecog.dev

runSbt () {
  "$(/usr/libexec/java_home -v 1.7)/bin/java" 2>target/sbt.err.log \
  $SBT_JAVA_OPTS \
  $(cat .jvmopts) \
  -Dsbt.global.base="$HOME/.sbt/$sbtVersion" \
  -jar "$HOME/.sbt/launchers/$sbtVersion/sbt-launch.jar" \
  "$@"
}

[[ -d target ]] || mkdir target
runSbt "$@"
# |& egrep --line-buffered -v '^(Ignoring unparsed versions)'
