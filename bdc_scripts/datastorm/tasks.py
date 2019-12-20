from bdc_scripts.celery import celery_app


@celery_app.task()
def warp():
    pass


@celery_app.task()
def merge():
    pass


@celery_app.task()
def blend():
    pass


@celery_app.task()
def publish():
    pass