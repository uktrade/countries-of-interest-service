#!/bin/bash

source ./scripts/functions.sh

run "pip install --upgrade pip"
run 'pip install -r /tmp/requirements.txt'
