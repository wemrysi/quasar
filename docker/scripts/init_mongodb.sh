#!/bin/bash
set -e

echo "configuring mongodb..."

mongo localhost:27017/quasar-test --eval 'db.createUser({"user": "quasar-dbOwner", "pwd": "quasar", "roles": [ "dbOwner" ]})'
mongo localhost:27017/quasar-test --eval 'db.createUser({"user": "quasar-read", "pwd": "quasar", "roles": [ "read" ]})'
