#!/bin/bash
set -e

##########################################
# generic function to init containerized 
# connectors. init scripts are copied to
# the container and ran within the container
#
init_containerdb() {
  CONTAINERNAME=$1
  DBNAME=$(echo "$CONTAINERNAME" | cut -d_ -f1)
  docker cp init_$DBNAME.sh $CONTAINERNAME:/tmp/init_$DBNAME.sh
  docker exec $CONTAINERNAME /tmp/init_$DBNAME.sh
}

##########################################
# methods to intialize various databases
#
init_mongo() {
  # only mongodb_read_only need to be configured
  echo "configuring mongodb_read_only..."
  init_containerdb mongodb_read_only
}

init_postgresql() {
  # a restart of the container is needed here due to
  # http://askubuntu.com/questions/308054/how-to-create-s-pgsql-5432
  echo "configuring postgresql..."
  docker restart postgresql
  init_containerdb postgresql
}

init_couchbase() {
  echo "configuring couchbase..."
  init_containerdb couchbase
}

init_marklogic() {
  # marklogic init script is ran from outside
  # the container due to this curl issue
  # curl: symbol lookup error: /lib64/libldap-2.4.so.2
  echo "configuring marklogic..."
  ./init_marklogic.sh $1 $2
}

##########################################
# parse arguments and run various
# database configuration scripts
#
apply_configuration() {
  CONNECTOR=$1
  if [[ $CONNECTOR == "mongodb_read_only" ]]; then init_mongo;                     fi
  if [[ $CONNECTOR =~ "couchbase"         ]]; then init_couchbase;                 fi
  if [[ $CONNECTOR == "marklogic_xml"     ]]; then init_marklogic $DOCKERIP 8001;  fi
  if [[ $CONNECTOR == "marklogic_json"    ]]; then init_marklogic $DOCKERIP 9001;  fi
  if [[ $CONNECTOR =~ "postgresql"        ]]; then init_postgresql;                fi
}

configure_connectors() {
  for CONNECTOR in $1
    do
      apply_configuration $CONNECTOR
  done
}


##########################################
# attach our shell to docker
# this allows us to run docker commands
#
enable_docker_env() {
  if [[ -x "$(command -v docker-machine)" ]]
  then
    echo "found docker-machine, adding it to env..."
    eval "$(docker-machine env default)"
    DOCKERIP=$(docker-machine ip default)
  else
    if [[ -x "$(command -v docker)" ]]
    then
      echo "docker is in your path, proceeding..."
      DOCKERIP=localhost
    else
      echo "docker needs to be installed in order to run: $0"
      exit 1
    fi
  fi
}

find_connectors() {
 CONNECTORS=$(docker ps --filter "name=" | awk '{if(NR>1) print $NF}')
}

configure_all_live_dockerized_connectors() {
  if [[ ${TRAVIS:-} ]]
  then
    echo "in a travis environment, docker is in our path..."
  else
    echo "local environment, looking for docker..."
    find_connectors
    configure_connectors "$CONNECTORS"
  fi
}

create_database() {
  docker-compose up -d $1 
}

usage() {
cat << EOF
Usage: $0 [-h] [-a] [-c CONNECTOR-NAME]
Create and configure dockerized mongo, couchbase, marklogic, and postgresql
connectors for integration tests with Quasar.

  -h                   help (also trigged with no parameters): display this help and exit
  -a                   configure all currently running dockerized connectors
  -c CONNECTOR-NAME    configure running dockerized connector named CONNECTOR-NAME
  -u "con1 con2..."    use docker-compose up to create and configure a quoted list of connectors
EOF
}

# if no args are passed in print usage
[ $# -eq 0 ] && usage

# command line parsing logic
while getopts ":hac:u:" opt; do
  case $opt in
    a)
      echo "configuring all connectors..." >&2
      enable_docker_env
      configure_all_live_dockerized_connectors
      ;;
    c)
      echo "$OPTARG is being configured..." >&2
      enable_docker_env
      if [[ $OPTARG =~ "spark" ]]
      then
        echo "Will not make a container for this connector: $OPTARG"
      else
        apply_configuration $OPTARG
      fi     
      ;;
    u)
      echo "bringing the following connectors: $OPTARG" >&2
      enable_docker_env
      for CONNECTOR in $OPTARG
      do
        create_database $CONNECTOR
        sleep 5
        apply_configuration $CONNECTOR
      done
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
