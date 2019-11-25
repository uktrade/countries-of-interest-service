# import etl.tasks
# from app import my_celery_task
# from flask import request


# def populate_database():
#     drop_table = 'drop-table' in request.args
#     # populate_database.delay(drop_table)
#     my_celery_task.delay(drop_table)
#     return {'status': 200}
