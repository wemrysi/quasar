#!/usr/bin/env bash

set -euo pipefail # STRICT MODE
IFS=$'\n\t'       # http://redsymbol.net/articles/unofficial-bash-strict-mode/

end=$((SECONDS+1200))

# wait for 20 mins
while [ $SECONDS -lt $end ]; do
    :
done
