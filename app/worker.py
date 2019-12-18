from app.application import get_or_create

app = get_or_create()
celery_app = app.celery
