#!/usr/bin/env bash
set -euo pipefail # STRICT MODE
IFS=$'\n\t'       # http://redsymbol.net/articles/unofficial-bash-strict-mode/


CONNECTOR=$1
if [[ $CONNECTOR == "spark_local_test" ]]
then
    echo "$CONNECTOR: not starting a container for this connector..."
else
  cd $TRAVIS_BUILD_DIR
  docker-compose -f ./docker/docker-compose.yml up -d $CONNECTOR
fi
