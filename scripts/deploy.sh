#!/bin/bash

source ./scripts/functions.sh

PARAMETERS="-m 4G -k 3G"
SYSTEM="-$1"

if [ "$1" = "live" ]; then
  PARAMETERS="-m 4G -k 4G"
  SYSTEM=""
fi

run "cf push -f manual-manifest.yml countries-of-interest-service-$1 $PARAMETERS --no-start"
run "cf start countries-of-interest-service$SYSTEM"
