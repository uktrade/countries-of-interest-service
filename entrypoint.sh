#!/bin/bash

# run the scheduled task to populate the database in the celery app
RUN_SCHEDULER=True celery -A app.celery worker --loglevel=info &
if [[ -z $VCAP_SERVICES ]]; then
    if [ $FLASK_ENV == "production" ]; then
	gunicorn app:app -b 0.0.0.0:5000
    else
	python app.py
    fi
else
    npm run build;
    gunicorn app:app
fi
