from celery import Celery


app = Celery(__name__,
             backend='rpc://',
             broker='pyamqp://guest@localhost')