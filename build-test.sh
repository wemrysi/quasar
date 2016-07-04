#!/usr/bin/env bash
#

cd "$(dirname $0)"

# stolen shamelessly from start-bifrost.sh
port_is_open() {
	netstat -an | egrep "[\.:]$1[[:space:]]+.*LISTEN" > /dev/null
}

wait_until_port_open() {
    while port_is_open $1; do
        sleep 1
    done
}

SUCCESS=0
FAILEDTARGETS=""
SBT_JAVA_OPTS=""
# SBT_JAVA_OPTS="-Dsbt.log.noformat=true"

function run_sbt() {
    echo "SBT Run: $@"
    SBT_JAVA_OPTS="$SBT_JAVA_OPTS" ./sbt "$@"
    if [[ $? != 0 ]]; then
        SUCCESS=1
        for TARGET in $@; do
            FAILEDTARGETS="$TARGET $FAILEDTARGETS"
        done
    fi
}

while getopts ":m:saokcf" opt; do
    case $opt in
        k)
            SKIPCLEAN=1
            ;;
        s)
            SKIPSETUP=1
            ;;
        a)
            SKIPTEST=1
            ;;
        o)
            SBT_JAVA_OPTS="$SBT_JAVA_OPTS -Dcom.precog.build.optimize=true"
            ;;
	c)
	    COVERAGE=1
	    ;;
	f)
	    FAILFAST=1
	    ;;
        \?)
            echo "Usage: `basename $0` [-o] [-s] [-k] [-m <mongo port>]"
            echo "  -o: Optimized build"
            echo "  -s: Skip all clean/compile setup steps"
            echo "  -k: Skip sbt clean step"
    	    echo "  -f: Fail fast if a module test fails"
            exit 1
            ;;
    esac
done

# enable errexit (jenkins does it, but not shell)
set -e

if [ -z "$SKIPSETUP" ]; then
    [ -z "$SKIPCLEAN" ] && find . -type d -name target -prune -exec rm -fr {} \;
    [ -z "$SKIPCLEAN" ] && run_sbt clean

    run_sbt compile

    if [ -z "$SKIPTEST" ]; then
        run_sbt "test:compile"
    fi
else
    echo "Skipping clean/compile"
fi

# For the runs, we don't want to terminate early if a particular project fails
if [ -z "$FAILFAST" ]
then
    echo "Build will fail on first failed subproject"
	set +e
fi

if [ -z "$SKIPTEST" ]; then
    for bsbt in */build.sbt ; do
        run_sbt "$(dirname $bsbt)/test"
    done
else
    echo "Skipping test:compile/test"
fi

# re-enable errexit
set -e

# For the triggers
if [ $SUCCESS -eq 0 ]; then
  echo "Finished: SUCCESS"
else
  echo "Finished: FAILURE"
  echo "Failed targets: $FAILEDTARGETS"
fi

exit $SUCCESS
