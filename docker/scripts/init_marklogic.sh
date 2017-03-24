#!/usr/bin/env bash
set -euo pipefail # STRICT MODE
IFS=$'\n\t'       # http://redsymbol.net/articles/unofficial-bash-strict-mode/

ML_ADMIN_PREFIX="http://localhost:8001/admin/v1"
ML_USERNAME="marklogic"
ML_PASSWORD="marklogic"

# Initialize the server with the license info
curl -d "" ${ML_ADMIN_PREFIX}/init

# Allow the server to restart
sleep 20

# Add the admin user
curl -d "admin-username=${ML_USERNAME}&admin-password=${ML_PASSWORD}" ${ML_ADMIN_PREFIX}/instance-admin
