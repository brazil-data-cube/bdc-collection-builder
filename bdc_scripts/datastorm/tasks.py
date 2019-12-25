from bdc_scripts.celery import celery_app


@celery_app.task()
def warp(datacube, asset):
    print('Execute Warp of {} - Asset {}'.format(datacube, asset.get('url')))


@celery_app.task()
def merge():
    pass


@celery_app.task()
def blend():
    pass


@celery_app.task()
def publish():
    pass