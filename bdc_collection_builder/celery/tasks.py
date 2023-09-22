#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2022 INPE.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/gpl-3.0.html>.
#

"""Module to deal with Celery Tasks."""

import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from bdc_catalog.models import Collection, Item, db
from bdc_collectors.base import BaseCollection
from bdc_collectors.exceptions import DataOfflineError
from celery import current_app, current_task
from celery.backends.database import Task
from sentinelsat.exceptions import InvalidChecksumError

from ..collections.collect import get_provider_order
from ..collections.models import RadcorActivity, RadcorActivityHistory
from ..collections.processor import sen2cor
from ..collections.utils import (get_or_create_model, get_provider,
                                 is_valid_compressed_file, post_processing, safe_request)
from ..config import Config
from .publish import get_item_path, publish_collection_item


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

        defaults = dict(
            tags=activity.get('tags', []),
            scene_type=activity.get('scene_type'),
            sceneid=activity['sceneid']
        )

        activity.pop('history', None)
        activity.pop('children', None)
        activity.pop('parents', None)
        activity.pop('id', None)
        activity.pop('last_execution', None)

        activity_model, _ = get_or_create_model(RadcorActivity, defaults=defaults, **where)

        model = RadcorActivityHistory()

        task, _ = get_or_create_model(Task, defaults={}, task_id=current_task.request.id)
        db.session.add(task)
        logging.info(f"Task {task} - {task.task_id}")

        model.task = task
        model.activity = activity_model
        model.start = datetime.utcnow()

    # Ensure that args values is always updated
    copy_args = dict(**model.activity.args or {})
    copy_args.update(activity['args'] or dict())
    model.activity.args = copy_args

    model.save()

    return model


def execution_from_collection(activity, collection_id=None, activity_type=None):
    """Create an task execution context and set the collection."""
    if collection_id:
        activity['collection_id'] = collection_id

    if activity_type:
        activity['activity_type'] = activity_type

    return create_execution(activity)


def get_provider_collection(provider_name: str, dataset: str) -> BaseCollection:
    """Retrieve a data collector class instance from given bdc-collector provider."""
    provider_setting, collection = get_provider(provider_name)

    return collection.get_collector(dataset)


def get_provider_collection_from_activity(activity: dict) -> BaseCollection:
    """Retrieve an instance of bdc_collectors.base.BaseCollection."""
    return get_provider_collection(activity['args']['catalog'], activity['args']['dataset'])(activity['sceneid'])


def refresh_execution_args(execution: RadcorActivityHistory, activity: dict, **kwargs):
    """Update new values to the execution activity."""
    copy_args = execution.activity.args.copy()
    copy_args.update(**kwargs)
    activity['args'] = copy_args

    execution.activity.args = copy_args
    execution.save()


@current_app.task(
    queue=os.getenv('QUEUE_DOWNLOAD', 'download'),
    max_retries=int(os.getenv("TASK_RETRY_COUNT", "72")),
    autoretry_for=(DataOfflineError, InvalidChecksumError,),
    default_retry_delay=Config.TASK_RETRY_DELAY
)
def download(activity: dict, **kwargs):
    """Celery tasks to deal with download data product from given providers."""
    execution = create_execution(activity)

    collection: Collection = execution.activity.collection
    scene_id = execution.activity.sceneid
    catalog_args = activity['args'].get('catalog_args', dict())

    logging.info(f'Starting Download Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    if len(catalog_args) > 0:
        catalog_name = activity['args']['catalog']
        catalog_args.update(parallel=True, progress=False, lazy=True)

        provider, collector = get_provider(catalog=catalog_name, **catalog_args)
        setattr(collector, 'instance', provider)
        setattr(collector, 'provider_name', f'{provider.driver_name} (CUSTOM)')
        download_order = [collector]
    else:
        # Use parallel flag for providers which has number maximum of connections per client (Sentinel-Hub only)
        download_order = get_provider_order(collection, lazy=True, parallel=True, progress=False,
                                            **catalog_args)

    if len(download_order) == 0:
        raise RuntimeError(f'No provider set for collection {collection.id}({collection.name})')

    data_collection = get_provider_collection_from_activity(activity)

    prefix = Config.DATA_DIR
    if collection.collection_type == 'cube':
        prefix = Config.CUBES_DATA_DIR

    download_file = data_collection.compressed_file(collection, prefix=prefix, path_include_month=activity['args']['path_include_month'])

    has_compressed_file = download_file is not None

    # For files that does not have compressed file (Single file/folder), use native path
    if download_file is None:
        download_file = data_collection.path(collection, path_include_month=activity['args']['path_include_month'])

    is_valid_file = False

    item = Item.query().filter(
        Item.collection_id == collection.id,
        Item.name == scene_id
    ).first()

    if item and item.assets.get('asset'):
        # TODO: Get asset name of download file
        item_path = item.assets['asset'].get('href', '')
        item_path = item_path if not item_path.startswith('/') else item_path[1:]
        item_path = Path(prefix) / item_path

        if item_path.exists():
            logging.info(f'Item {scene_id} exists. {str(item_path)} -> {str(download_file)}')
            download_file = item_path

    if download_file.exists() and has_compressed_file:
        logging.info('File {} downloaded. Checking file integrity...'.format(str(download_file)))
        # TODO: Should we validate using Factory Provider.is_valid() ?
        is_valid_file = is_valid_compressed_file(str(download_file)) if download_file.is_file() else False

    if not download_file.exists() or not is_valid_file:
        # Ensure file is removed since it may be corrupted
        if download_file.exists() and download_file.is_file():
            download_file.unlink()

        if not has_compressed_file:
            download_file.mkdir(exist_ok=True, parents=True)
        else:
            download_file.parent.mkdir(exist_ok=True, parents=True)

        with TemporaryDirectory(prefix='download_', suffix=f'_{scene_id}', dir=Config.WORKING_DIR) as tmp:
            temp_file: Path = None

            should_retry = False

            for collector in download_order:
                try:
                    logging.info(f'Trying to download from {collector.provider_name}(id={collector.instance.id})')

                    options = dict()
                    options['glob_pattern'] = activity['args']['glob_pattern']

                    with safe_request():
                        temp_file = Path(collector.download(scene_id, output=tmp, kwargs=options))

                    activity['args']['provider_id'] = collector.instance.id

                    break
                except (DataOfflineError, InvalidChecksumError):
                    should_retry = True
                except Exception as e:
                    logging.error(f'Download error in provider {collector.provider_name} - {str(e)}')

            if temp_file is None or not temp_file.exists():
                if should_retry:
                    raise DataOfflineError(scene_id)
                raise RuntimeError(f'Download fails {activity["sceneid"]}.')

            shutil.move(str(temp_file), str(download_file))
        if tmp and Path(tmp).exists():
            logging.info(f'Cleaning up {tmp}')
            shutil.rmtree(tmp)

    refresh_execution_args(execution, activity, compressed_file=str(download_file))

    return activity


@current_app.task(queue=os.getenv('QUEUE_PROCESSOR', 'correction'))
def correction(activity: dict, collection_id=None, **kwargs):
    """Celery task to deal with Surface Reflectance processors."""
    execution = execution_from_collection(activity, collection_id=collection_id, activity_type=correction.__name__)

    collection: Collection = execution.activity.collection
    scene_id = activity['sceneid']

    logging.info(f'Starting Correction Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    data_collection = get_provider_collection_from_activity(activity)
    tmp = None

    try:
        output_path = data_collection.path(collection, prefix=Config.PUBLISH_DATA_DIR, path_include_month=activity['args']['path_include_month'])

        if collection.metadata_ and collection.metadata_.get('processors'):
            processor_name = collection.metadata_['processors'][0]['name']

            with TemporaryDirectory(prefix='correction_', suffix=f'_{scene_id}', dir=Config.WORKING_DIR) as tmp:
                shutil.unpack_archive(activity['args']['compressed_file'], tmp)

                # Process environment
                env = dict(**os.environ, INDIR=str(tmp), OUTDIR=str(output_path))

                entry = scene_id
                entries = list(Path(tmp).iterdir())

                if len(entries) == 1 and entries[0].suffix == '.SAFE':
                    entry = entries[0].name

                output_path.mkdir(exist_ok=True, parents=True)

                container_workdir = activity['args'].get('container_workdir', kwargs.get('container_workdir', ''))
                if not container_workdir:
                    container_workdir = Config.CONTAINER_WORKDIR

                container_workdir = f'-v {container_workdir}' if container_workdir else ''

                if processor_name.lower() == 'sen2cor':
                    fragments = scene_id.split('_')
                    tile = fragments[5]
                    sensing_date = fragments[2]

                    for output_path_entry in output_path.iterdir():
                        entry_fragments = output_path_entry.stem.split('_')
                        sensor_product = entry_fragments[1] if len(entry_fragments) else None

                        is_sen2cor_file = output_path_entry.stem.startswith(f'{tile}_{sensing_date}')

                        if output_path_entry.name.startswith('S2') and sensor_product == 'MSIL2A':
                            logging.info(f'Found {str(output_path_entry)} generated before. Removing it.')
                            shutil.rmtree(output_path_entry, ignore_errors=True)
                        if is_sen2cor_file:
                            logging.info(f'Removing {str(output_path_entry)} sen2cor file before.')
                            output_path_entry.unlink()

                    env['OUTDIR'] = str(Path(tmp) / 'output')

                    sen2cor(scene_id, input_dir=str(tmp), output_dir=env['OUTDIR'],
                            docker_container_work_dir=container_workdir.split(' '),
                            timeout=kwargs.get('timeout'), **env)

                    logging.info(f'Using {entry} of sceneid {scene_id}')
                else:
                    lasrc_conf = Config.LASRC_CONFIG

                    cmd = f'''docker run --rm -i \
                        -v $INDIR:{Config.LASRC_CONFIG["LASRC_CONTAINER_INPUT_DIR"]} \
                        -v $OUTDIR:{Config.LASRC_CONFIG["LASRC_CONTAINER_OUTPUT_DIR"]} \
                        -v {lasrc_conf["LASRC_AUX_DIR"]}:/mnt/lasrc-aux:ro \
                        -v {lasrc_conf["LEDAPS_AUX_DIR"]}:/mnt/ledaps-aux:ro \
                        {container_workdir} {lasrc_conf["LASRC_DOCKER_IMAGE"]} {entry}'''

                    logging.debug(cmd)

                    # Execute command line
                    process = subprocess.Popen(cmd, shell=True, env=env, stdin=subprocess.PIPE)
                    process.wait()

                    assert process.returncode == 0

                # TODO: We should be able to get output name from execution
                if processor_name.lower() == 'sen2cor':
                    # Since sen2cor creates an custom directory name (based in scene_id) and changing processing date
                    # we create it inside "output" folder. After that, get first entry of that directory
                    output_tmp = list(Path(env['OUTDIR']).iterdir())[0]

                    output_path = output_path / output_tmp.name

                    shutil.move(output_tmp, output_path)

                refresh_execution_args(execution, activity, file=str(output_path))
        else:
            raise RuntimeError(f'Processor not supported. Check collection {collection.name} metadata processors')
    except Exception as e:
        logging.error(f'Error in correction {scene_id} - {str(e)}', exc_info=True)
        raise e
    finally:
        if tmp and Path(tmp).exists():
            logging.info(f'Cleaning up {tmp}')
            shutil.rmtree(tmp)

    return activity


@current_app.task(
    queue=os.getenv('QUEUE_PUBLISH', 'publish'),
    max_retries=int(os.getenv("TASK_RETRY_COUNT", "72")),
    default_retry_delay=Config.TASK_RETRY_DELAY
)
def publish(activity: dict, collection_id=None, **kwargs):
    """Celery tasks to publish an item on database."""
    execution = execution_from_collection(activity, collection_id=collection_id, activity_type=publish.__name__)

    collection = execution.activity.collection

    scene_id = activity['sceneid']

    logging.info(f'Starting Publish Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    try:
        data_collection = get_provider_collection_from_activity(activity)

        file = activity['args'].get('file') or activity['args'].get('compressed_file')

        refresh_execution_args(execution, activity, file=str(file))

        options = activity['args']
        options.update(**kwargs)

        provider_id = activity['args'].get('provider_id')

        publish_collection_item(scene_id, data_collection, collection, file,
                                cloud_cover=activity['args'].get('cloud'),
                                scene_meta=activity['args'].get("scene_meta"),
                                provider_id=provider_id, publish_hdf=options.get('publish_hdf'), activity=execution.activity.args)

        if file:
            refresh_execution_args(execution, activity, file=str(file))
    except RuntimeError as e:
        logging.error(f'Error in publish {scene_id} - {str(e)}', exc_info=True)
        raise

    return activity


@current_app.task(queue=os.getenv('QUEUE_POST_PROCESSING', 'post'))
def post(activity: dict, collection_id=None, **kwargs):
    """Celery task to deal with data post processing."""
    execution = execution_from_collection(activity, collection_id=collection_id, activity_type=post.__name__)

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

    # TODO: Look in bands and get resolution
    resample = None

    if activity['sceneid'].startswith('S2'):
        resample = 10

    post_processing(quality_path, collection, scenes, resample_to=resample)
    # TODO: Create new band

    return activity

