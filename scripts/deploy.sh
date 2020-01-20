#!/bin/bash

source ./scripts/functions.sh

PARAMETERS="-m 2G -k 2G"

if [ "$1" = "live" ]; then
  PARAMETERS="-m 4G -k 4G"
fi

run "cf push -f manual-manifest.yml countries-of-interest-service-$1 $PARAMETERS --no-start"
run "cf start countries-of-interest-service-$1"
