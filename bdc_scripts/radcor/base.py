from celery import current_task
from bdc_scripts.celery import celery_app
from bdc_scripts.radcor.models import RadcorActivity


class RadcorTask(celery_app.Task):
    def get_activity(self):
        task_id = current_task.request.id

        return RadcorActivity.get_by_task_id(task_id)
