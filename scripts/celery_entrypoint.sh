#!/bin/bash

source ./scripts/functions.sh

PATH=/root/miniconda3/envs/countries-of-interest-service/bin:${PATH}

run "make run_dev_celery"
