import atexit
from apscheduler.schedulers.background import BackgroundScheduler
from views import populate_database

class Scheduler:

    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.add_job(populate_database, 'cron', hour='0')
        atexit.register(lambda: self.scheduler.shutdown(wait=False))

    def start(self):
        self.scheduler.start()

