#!/bin/bash

celery -A etl.tasks worker --loglevel=info &
python app.py
