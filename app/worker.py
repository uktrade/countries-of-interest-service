from app.api.tasks import populate_database_task
from app.application import get_or_create
from app.etl.scheduler import Scheduler

app = get_or_create()
celery_app = app.celery

if app.config['app']['run_scheduler']:
    scheduled_task = Scheduler(populate_database_task)
    scheduled_task.start()
