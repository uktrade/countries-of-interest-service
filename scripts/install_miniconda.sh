#!/bin/bash

source ./scripts/functions.sh

run "curl https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh  --output miniconda.sh"
run "bash miniconda.sh -b"
run "rm -f miniconda.sh"