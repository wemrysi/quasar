# This script examines the running containers
# and produces a testing.conf file with proper URIs
#
#!/usr/bin/env bash

set -euo pipefail # STRICT MODE
IFS=$'\n\t'

##########################################
# injects spark_local path into
# it/testing.conf
# need to call it like this if running locally
# buildtestingconfig.sh spark
#
configure_spark() {
  echo "spark_local=\"${HOME}/spark_local_test/\"" >> $CONFIGFILE
}


##########################################
# injects marklogic xml and json url
# into it/testing.conf
#
configure_marklogic() {
  CONTAINERNAME=$1
  if [[ $CONTAINERNAME == "marklogic" ]]
  then 
    echo "marklogic_json=\"xcc://marklogic:marklogic@${DOCKERIP}:8000/Documents?format=json\"" >> $CONFIGFILE
    echo "marklogic_xml=\"xcc://marklogic:marklogic@${DOCKERIP}:8000/Documents?format=xml\"" >> $CONFIGFILE
  fi  
}

##########################################
# injects couchbase url into
# it/testing.conf
#
configure_couchbase() {
  CONTAINERNAME=$1
  if [[ $CONTAINERNAME == "couchbase" ]]; then echo "$CONTAINERNAME=\"couchbase://${DOCKERIP}?username=Administrator&password=password\"" >> $CONFIGFILE; fi  
}


##########################################
# injects postgresql url into
# it/testing.conf
#
configure_postgresql() {
  CONTAINERNAME=$1
  if [[ $CONTAINERNAME == "postgresql" ]]; then echo "$CONTAINERNAME=\"jdbc:postgresql://${DOCKERIP}/quasar-test?user=postgres&password=postgres\"" >> $CONFIGFILE; fi  
}

##########################################
# injects various mongodb urls into
# it/testing.conf depending on argument
#
configure_mongo() {
  CONTAINERNAME=$1
  if [[ $CONTAINERNAME == "mongodb_2_6"       ]]; then echo "$CONTAINERNAME=\"mongodb://${DOCKERIP}:27018\"" >> $CONFIGFILE; fi 
  if [[ $CONTAINERNAME == "mongodb_3_0"       ]]; then echo "$CONTAINERNAME=\"mongodb://${DOCKERIP}:27019\"" >> $CONFIGFILE; fi 
  if [[ $CONTAINERNAME == "mongodb_read_only" ]]; then echo "$CONTAINERNAME=\"mongodb://${DOCKERIP}:27020\"" >> $CONFIGFILE; fi 
  if [[ $CONTAINERNAME == "mongodb_3_2"       ]]; then echo "$CONTAINERNAME=\"mongodb://${DOCKERIP}:27021\"" >> $CONFIGFILE; fi 
  if [[ $CONTAINERNAME == "mongodb_3_4"       ]]; then echo "$CONTAINERNAME=\"mongodb://${DOCKERIP}:27022\"" >> $CONFIGFILE; fi 
}


insert_bogus() {
  echo "bogus=true" > $CONFIGFILE
}

connector_lookup() {
  CONNECTOR=$1
  if [[ $CONNECTOR =~ "mongodb"              ]]; then configure_mongo      $CONTAINER; fi
  if [[ $CONNECTOR =~ "postgresql"           ]]; then configure_postgresql $CONTAINER; fi
  if [[ $CONNECTOR =~ "marklogic"            ]]; then configure_marklogic  $CONTAINER; fi
  if [[ $CONNECTOR =~ "couchbase"            ]]; then configure_couchbase  $CONTAINER; fi
  if [[ $CONNECTOR == "spark_local_test"     ]]; then configure_spark; fi
}

populate_travis_testing_config() {
  CONNECTOR=$1
  CONFIGFILE=$TRAVIS_BUILD_DIR/it/testing.conf
  rm -f $CONFIGFILE
  insert_bogus
  connector_lookup $CONNECTOR
}

populate_local_testing_config() {
  CONNECTORS=$(docker ps --filter "name=" | awk '{if(NR>1) print $NF}')
  CONFIGFILE=../../it/testing.conf
  rm -f $CONFIGFILE
  insert_bogus
  for CONNECTOR in $CONNECTORS
  do
    connector_lookup $CONNECTOR
  done
}

##########################################
# main method
# it determines if we are running in travis or not
# once it figures out where it is running it 
# proceeds to configure the various items we need
# to run this script
#
if [[ ${TRAVIS:-} ]]
then
  echo "creating testing.conf in a travis env..."
  DOCKERIP="localhost"
  populate_travis_testing_config $1
else
  echo "creating testing.conf in a local env..."
  # check if we are running docker-machine on a mac if so run eval
  # if not then maybe we are in linux and docker-machine doesn't exist
  # we need this in order to run docker commands
  if [[ $(command -v docker-machine) = 0 ]]
  then 
    eval $(docker-machine env default)
    DOCKERIP=$(docker-machine ip default)
  else
    DOCKERIP="localhost"
  fi
  if [[ $# -gt 1 && $1 == "spark" ]]; then configure_spark; fi
  populate_local_testing_config $CONNECTORS
fi
