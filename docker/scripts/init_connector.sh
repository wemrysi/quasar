#!/bin/bash
set -ev

CONNECTOR=$1
if [[ $CONNECTOR == "spark_local_test" ]]
then
  if [[ ${TRAVIS:-} ]]
  then
    CONFIGFILE=$TRAVIS_BUILD_DIR/it/testing.conf
    echo "spark_local=\"${HOME}/spark_local_test/\"" >> $CONFIGFILE
  fi
else
  cd $TRAVIS_BUILD_DIR
  docker-compose -f ./docker/docker-compose.yml up -d $CONNECTOR
fi
