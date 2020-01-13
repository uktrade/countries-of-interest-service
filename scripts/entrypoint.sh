#!/bin/bash

source ./scripts/functions.sh

PATH=/root/miniconda3/envs/countries-of-interest-service/bin:${PATH}
# run the scheduled task to populate the database in the celery app
run "python manage.py dev db --create_tables"
run "make run_dev_server"
