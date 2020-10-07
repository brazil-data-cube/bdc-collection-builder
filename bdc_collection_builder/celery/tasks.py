import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from bdc_catalog.models import Provider, Item
from bdc_collectors.base import BaseCollection
from bdc_collectors.exceptions import DataOfflineError
from celery import current_app, current_task
from celery.backends.database import Task
from flask import current_app as flask_app

from ..collections.models import RadcorActivityHistory, RadcorActivity
from ..collections.utils import get_or_create_model, is_valid_compressed_file, post_processing
from ..config import Config
from .publish import publish_collection, get_item_path


def create_execution(activity):
    """Create a radcor activity once a celery task is running.

    Args:
        activity (dict) - Radcor activity as dict
    """
    model = RadcorActivityHistory.query().filter(
        RadcorActivityHistory.task.has(task_id=current_task.request.id)
    ).first()

    if model is None:
        where = dict(
            sceneid=activity.get('sceneid'),
            activity_type=activity.get('activity_type'),
            collection_id=activity.get('collection_id')
        )

        activity.pop('history', None)
        activity.pop('id', None)
        activity.pop('last_execution', None)

        activity_model, _ = get_or_create_model(RadcorActivity, defaults=activity, **where)

        model = RadcorActivityHistory()

        task, _ = get_or_create_model(Task, defaults={}, task_id=current_task.request.id)

        model.task = task
        model.activity = activity_model
        model.start = datetime.utcnow()

    # Ensure that args values is always updated
    model.activity.args = activity['args']

    model.save()

    return model


def execution_from_collection(activity, collection_id=None):
    """Create an task execution context and set the collection."""
    if collection_id:
        activity['collection_id'] = collection_id

    return create_execution(activity)


def get_provider_collection(provider_name: str, dataset: str) -> BaseCollection:
    collector_extension = flask_app.extensions['bdc:collector']

    provider_class = collector_extension.get_provider(provider_name)

    instance = Provider.query().filter(Provider.name == provider_name).first()

    if instance is None:
        raise Exception(f'Provider {provider_name} not found.')

    if isinstance(instance.credentials, dict):
        provider = provider_class(**instance.credentials)
    else:
        provider = provider_class(instance.credentials)

    collection = provider.get_collector(dataset)

    return collection


def get_provider_collection_from_activity(activity: dict) -> BaseCollection:
    return get_provider_collection(activity['args']['catalog'], activity['args']['dataset'])(activity['sceneid'])


@current_app.task(
    queue='download',
    max_retries=72,
    autoretry_for=(DataOfflineError,),
    default_retry_delay=Config.TASK_RETRY_DELAY
)
def download(activity: dict, **kwargs):
    execution = create_execution(activity)

    collector_extension = flask_app.extensions['bdc:collector']

    collection = execution.activity.collection
    scene_id = execution.activity.sceneid

    logging.info(f'Starting Download Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    # Use parallel flag for providers which has number maximum of connections per client (Sentinel-Hub only)
    download_order = collector_extension.get_provider_order(collection, parallel=True)

    if len(download_order) == 0:
        raise RuntimeError(f'No provider set for collection {collection.id}({collection.name})')

    data_collection = get_provider_collection_from_activity(activity)

    download_file = data_collection.compressed_file(collection)

    is_valid_file = False

    if download_file.exists():
        logging.info('File {} downloaded. Checking file integrity...'.format(str(download_file)))
        # TODO: Should we validate using Factory Provider.is_valid() ?
        is_valid_file = is_valid_compressed_file(str(download_file))

    if not download_file.exists() or not is_valid_file:
        # Ensure file is removed since it may be corrupted
        download_file.unlink(missing_ok=True)

        download_file.parent.mkdir(exist_ok=True, parents=True)

        with TemporaryDirectory(prefix='download_', suffix=f'_{scene_id}') as tmp:
            temp_file: Path = None

            should_retry = False

            for collector in download_order:
                try:
                    logging.info(f'Trying to download from {collector.provider_name}(id={collector.instance.id})')
                    temp_file = Path(collector.download(scene_id, output=tmp))

                    activity['args']['provider_id'] = collector.instance.id

                    break
                except DataOfflineError:
                    should_retry = True
                except Exception as e:
                    logging.error(f'Download error in provider {collector.provider_name} - {str(e)}')

            if temp_file is None or not temp_file.exists():
                if should_retry:
                    raise DataOfflineError(scene_id)
                raise RuntimeError('Download fails.')

            temp_file.rename(str(download_file))

    activity['args']['compressed_file'] = str(download_file)

    return activity


@current_app.task(queue='correction')
def correction(activity: dict, collection_id=None, **kwargs):
    execution = execution_from_collection(activity, collection_id=collection_id)

    collection = execution.activity.collection
    scene_id = activity['sceneid']

    logging.info(f'Starting Correction Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    data_collection = get_provider_collection_from_activity(activity)

    try:
        output_path = data_collection.path(collection)
        output_path.mkdir(exist_ok=True, parents=True)

        with TemporaryDirectory(prefix='correction_', suffix=f'_{scene_id}') as tmp:
            shutil.unpack_archive(activity['args']['compressed_file'], tmp)

            entries = list(Path(tmp).iterdir())

            entry = scene_id

            if len(entries) == 1 and entries[0].suffix == '.SAFE':
                entry = entries[0].name

            cmd = 'run_lasrc_ledaps_fmask.sh {}'.format(entry)

            logging.debug('cmd {}'.format(cmd))

            env = dict(**os.environ, INDIR=str(tmp), OUTDIR=str(output_path))
            process = subprocess.Popen(cmd, shell=True, env=env, stdin=subprocess.PIPE)
            process.wait()

            assert process.returncode == 0

            activity['args']['file'] = str(output_path)
    except Exception as e:
        logging.error(f'Error in correction {scene_id} - {str(e)}', exc_info=True)
        raise e

    return activity


@current_app.task(queue='publish')
def publish(activity: dict, collection_id=None, **kwargs):
    execution = execution_from_collection(activity, collection_id=collection_id)

    collection = execution.activity.collection

    scene_id = activity['sceneid']

    logging.info(f'Starting Publish Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    try:
        data_collection = get_provider_collection_from_activity(activity)

        publish_collection(scene_id, data_collection, collection, activity['args'].get('cloud'))
    except RuntimeError as e:
        logging.error(f'Error in publish {scene_id} - {str(e)}', exc_info=True)
        raise

    return activity


@current_app.task(queue='post')
def post(activity: dict, collection_id=None, **kwargs):
    execution = execution_from_collection(activity, collection_id=collection_id)

    collection = execution.activity.collection

    scene_id = activity['sceneid']

    logging.info(f'Starting Post Processing Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    item = Item.query().filter(
        Item.name == activity['sceneid'],
        Item.collection_id == collection.id
    ).first_or_404()

    scenes = {}

    quality_path = None

    for asset_name, asset in item.assets.items():
        if asset_name in ('thumbnail',) or 'ndvi' in asset_name or 'evi' in asset_name:
            continue

        if asset_name == 'Fmask4':
            quality_path = get_item_path(asset['href'])
            continue

        scenes[asset_name] = get_item_path(asset['href'])

    post_processing(quality_path, collection, scenes)
    # TODO: Create new band

    return activity
