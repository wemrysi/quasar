#!/usr/bin/env bash
#

set -euo pipefail
unset SBT_OPTS JVM_OPTS JDK_HOME JAVA_HOME
: ${TRAVIS_SCALA_VERSION:=2.11.8}

ANSI_CLEAR="\033[0K"

travis_fold() {
  local action="$1"
  local name="$2"
  echo -en "travis_fold:${action}:${name}\r${ANSI_CLEAR}"
}
runSbt () {
  local foldid="$1" && shift
  travis_fold start $foldid
  ./sbt ++$TRAVIS_SCALA_VERSION -batch "$@" | ts '%H:%M:%.S'
  travis_fold end $foldid
}

runSbt "sbt-update" -v update
runSbt "sbt-compile" -v coverage compile
runSbt "sbt-test" -v coverage test
runSbt "sbt-coverageReport" -v coverageReport
