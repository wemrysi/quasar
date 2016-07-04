#!/usr/bin/env bash
#

sbtVersion="0.13.11"

"$(/usr/libexec/java_home -v 1.7)/bin/java" \
$SBT_JAVA_OPTS \
$(cat .jvmopts) \
-Dsbt.global.base="$HOME/.sbt/$sbtVersion" \
-jar "$HOME/.sbt/launchers/$sbtVersion/sbt-launch.jar" \
"$@"
