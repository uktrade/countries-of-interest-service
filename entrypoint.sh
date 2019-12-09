#!/bin/bash

PATH=/opt/conda/envs/countries-of-interest-service/bin:${PATH}
# run the scheduled task to populate the database in the celery app
make run_dev_server