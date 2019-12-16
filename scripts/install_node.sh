#!/bin/bash

source ./scripts/functions.sh

run "curl https://deb.nodesource.com/setup_10.x --output node.sh"
run "bash node.sh -b"
run "rm -f node.sh"
run "apt-get install -y nodejs"