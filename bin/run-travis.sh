#!/usr/bin/env bash
#

set -euo pipefail

unset SBT_OPTS JVM_OPTS JDK_HOME JAVA_HOME
: ${TRAVIS_SCALA_VERSION:=2.11.8}
: ${SBT_TARGET:=$*}
: ${SBT_TARGET:=test}

ANSI_CLEAR="\033[0K"

start_regex='\[info\][[:space:]]*([[:alnum:]]+Spec)$'
end_regex='\[info\][[:space:]]*Total for specification ([[:alnum:]]+Spec)$'
ignore_regex='\[info\][[:space:]]+(Resolving|Loading|Done|Attempting|Formatting|Updating)'

travis_fold() {
  local action="$1"
  local name="$2"
  # echo >&2 "travis_fold:${action}:${name}"
  echo -en "travis_fold:${action}:${name}\r${ANSI_CLEAR}"
}

inject_folds () {
  while read -r line; do
    [[ "$line" =~ $start_regex ]] && travis_fold start "${BASH_REMATCH[1]}"
    [[ "$line" =~ $ignore_regex ]] || printf "%s\n" "$line"
    [[ "$line" =~ $end_regex ]] && travis_fold end "${BASH_REMATCH[1]}"
  done

  echo ""
}

./sbt ++$TRAVIS_SCALA_VERSION -no-colors -batch -v "$SBT_TARGET" | inject_folds # | ts '%H:%M:%.S'
