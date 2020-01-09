from app.application import get_or_create
from app.etl.scheduler import Scheduler
from app.api.tasks import populate_database_task

app = get_or_create()
celery_app = app.celery

scheduled_task = Scheduler(populate_database_task)
scheduled_task.start()
