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
  if [[ $CONNECTOR =~ "mongodb"              ]]; then configure_mongo      $CONNECTOR; fi
  if [[ $CONNECTOR =~ "postgresql"           ]]; then configure_postgresql $CONNECTOR; fi
  if [[ $CONNECTOR =~ "marklogic"            ]]; then configure_marklogic  $CONNECTOR; fi
  if [[ $CONNECTOR =~ "couchbase"            ]]; then configure_couchbase  $CONNECTOR; fi
  if [[ $CONNECTOR == "spark_local_test"     ]]; then configure_spark; fi
}


define_needed_evn_vars() {
  if [[ ${TRAVIS:-} ]]
  then
    CONFIGFILE=$TRAVIS_BUILD_DIR/it/testing.conf
    DOCKERIP="localhost"
  else
    CONFIGFILE=../../it/testing.conf
    if [[ -x "$(command -v docker-machine)" ]]
    then
      eval "$(docker-machine env default)"
      DOCKERIP=$(docker-machine ip default)  
    else
      DOCKERIP="localhost"
    fi
  fi
}

cleanup_testing_conf_file() {
  rm -f $CONFIGFILE
}

usage() {
cat << EOF
Usage: $0 [-h] [-a] [-c CONNECTOR-NAME]
Assembles Quasar integration configuration file, it/testing.conf, from currently running 
containerized connectors or for the specific CONNECTOR-NAME passed in. Works for local
development and within travis-ci.

  -h                   help (also trigged with no parameters): display this help and exit
  -a                   cleans existing testing.conf and adds connector URIs to testing.conf for all currently running dockerized connectors
  -i CONNECTOR-NAME    add connector URI to existing testing.conf file
  -c CONNECTOR-NAME    cleans exisitng testing.conf and adds a connector URI for CONNECTOR-NAME currently running 
  -t                   create testing.conf with all currently running dockerized connectors plus spark local connector
EOF
}

# if no args are passed in print usage
[ $# -eq 0 ] && usage

# initialize our env
define_needed_evn_vars

# command line parsing logic
while getopts ":hastpc:i:" opt; do
  case $opt in
    a)
      echo "creating testing.conf with URIs for all running connectors..." >&2
      cleanup_testing_conf_file
      insert_bogus
      CONNECTORS=$(docker ps --filter "name=" | awk '{if(NR>1) print $NF}')
      for CONNECTOR in $CONNECTORS
      do
        connector_lookup $CONNECTOR
      done          
      ;;
    i)
      echo "adding URI for $OPTARG to testing.conf..." >&2
      connector_lookup $OPTARG
      ;;
    c)
      echo "creating testing.conf with URI for $OPTARG..." >&2
      cleanup_testing_conf_file
      insert_bogus
      connector_lookup $OPTARG     
      ;;
    t)
      echo "creating testing.conf with URIs for all running connectors plus URI for spark local..." >&2
      ./$0 -a
      ./$0 -i spark_local_test
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
    h | *)
      usage
      exit 1
      ;;
  esac
done
