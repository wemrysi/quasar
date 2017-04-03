#!/bin/bash
set -e

echo "configuring mongodb..."

mongo --eval "db.stats()"  # do a simple harmless command of some sort

RESULT=$?   # returns 0 if mongo eval succeeds

if [ $RESULT -ne 0 ]; then
    echo "mongodb not running..."
    exit 1
else
  echo "mongodb running, creating db and users..."
  mongo localhost:27017/quasar-test --eval 'db.createUser({"user": "quasar-dbOwner", "pwd": "quasar", "roles": [ "dbOwner" ]})'
  mongo localhost:27017/quasar-test --eval 'db.createUser({"user": "quasar-read", "pwd": "quasar", "roles": [ "read" ]})'
fi



