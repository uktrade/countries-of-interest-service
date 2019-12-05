
PORT ?= 5000

.PHONY: run_server
run_server:
	exec gunicorn app:app -b 0.0.0.0:${PORT}


.PHONY: run_dev_server
run_dev_server:
	make run_celery &
	FLASK_DEBUG=1 flask run --host 0.0.0.0 --port ${PORT}

.PHONY: run_celery
run_celery:
	celery worker -A app.celery -l info -O fair --prefetch-multiplier 1 -Q celery

.PHONY: run_tests
run_tests:
	FLASK_ENV=test python -m pytest -p no:sugar --cov