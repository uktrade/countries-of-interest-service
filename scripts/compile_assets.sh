#!/bin/bash

source ./scripts/functions.sh

run "npm install"
run "npm run-script build"
