#!/bin/bash

source ./scripts/functions.sh


# run the scheduled task to populate the database in the celery app
# run "python manage.py dev db --create_tables"
run "make run_server"
