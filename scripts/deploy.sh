#!/bin/bash

source ./scripts/functions.sh

MANIFEST="manifest-dev.yml"

if [ "$1" = "live" ]; then
  MANIFEST="manifest-live.yml"
fi

run "cf push -f $MANIFEST countries-of-interest-service-$1 --no-start"
run "cf start countries-of-interest-service-$1"
