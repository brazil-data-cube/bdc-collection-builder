import logging
import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from bdc_catalog.models import Provider, Item, Collection
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

        model.task = task
        model.activity = activity_model
        model.start = datetime.utcnow()

    # Ensure that args values is always updated
    copy_args = dict(**model.activity.args)
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
    collector_extension = flask_app.extensions['bdc:collector']

    provider_class = collector_extension.get_provider(provider_name)

    instance = Provider.query().filter(Provider.name == provider_name).first()

    if instance is None:
        raise Exception(f'Provider {provider_name} not found.')

    if isinstance(instance.credentials, dict):
        options = dict(**instance.credentials)
        options['lazy'] = True
        options['progress'] = False
        provider = provider_class(**options)
    else:
        provider = provider_class(*instance.credentials, lazy=True, progress=False)

    collection = provider.get_collector(dataset)

    return collection


def get_provider_collection_from_activity(activity: dict) -> BaseCollection:
    return get_provider_collection(activity['args']['catalog'], activity['args']['dataset'])(activity['sceneid'])


def refresh_execution_args(execution: RadcorActivityHistory, activity: dict, **kwargs):
    """Update new values to the execution activity."""
    copy_args = execution.activity.args.copy()
    copy_args.update(**kwargs)
    activity['args'] = copy_args

    execution.activity.args = copy_args
    execution.save()


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
    download_order = collector_extension.get_provider_order(collection, lazy=True, parallel=True, progress=False)

    if len(download_order) == 0:
        raise RuntimeError(f'No provider set for collection {collection.id}({collection.name})')

    data_collection = get_provider_collection_from_activity(activity)

    download_file = data_collection.compressed_file(collection)

    has_compressed_file = download_file is not None

    # For files that does not have compressed file (Single file/folder), use native path
    if download_file is None:
        download_file = data_collection.path(collection)

    is_valid_file = False

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

        with TemporaryDirectory(prefix='download_', suffix=f'_{scene_id}') as tmp:
            temp_file: Path = None

            should_retry = False

            for collector in download_order:
                try:
                    logging.info(f'Trying to download from {collector.provider_name}(id={collector.instance.id})')
                    temp_file = Path(collector.download(scene_id, output=tmp, dataset=activity['args']['dataset']))

                    activity['args']['provider_id'] = collector.instance.id

                    break
                except DataOfflineError:
                    should_retry = True
                except Exception as e:
                    logging.error(f'Download error in provider {collector.provider_name} - {str(e)}')

            if temp_file is None or not temp_file.exists():
                if should_retry:
                    raise DataOfflineError(scene_id)
                raise RuntimeError(f'Download fails {activity["sceneid"]}.')

            shutil.move(str(temp_file), str(download_file))

    refresh_execution_args(execution, activity, compressed_file=str(download_file))

    return activity


@current_app.task(queue='correction')
def correction(activity: dict, collection_id=None, **kwargs):
    execution = execution_from_collection(activity, collection_id=collection_id, activity_type=correction.__name__)

    collection: Collection = execution.activity.collection
    scene_id = activity['sceneid']

    logging.info(f'Starting Correction Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    data_collection = get_provider_collection_from_activity(activity)

    try:
        output_path = data_collection.path(collection)

        if collection._metadata and collection._metadata.get('processors'):
            processor_name = collection._metadata['processors'][0]['name']

            with TemporaryDirectory(prefix='correction_', suffix=f'_{scene_id}') as tmp:
                shutil.unpack_archive(activity['args']['compressed_file'], tmp)

                # Process environment
                env = dict(**os.environ, INDIR=str(tmp), OUTDIR=str(output_path))

                entry = scene_id
                entries = list(Path(tmp).iterdir())

                if len(entries) == 1 and entries[0].suffix == '.SAFE':
                    entry = entries[0].name

                if processor_name.lower() == 'sen2cor':
                    output_path.parent.mkdir(exist_ok=True, parents=True)

                    sen2cor_conf = Config.SEN2COR_CONFIG
                    logging.info(f'Using {entry} of sceneid {scene_id}')
                    # TODO: Use custom sen2cor version (2.5 or 2.8)
                    cmd = f'''docker run --rm -i \
                        -v $INDIR:/mnt/input-dir \
                        -v $OUTDIR:/mnt/output-dir \
                        -v {sen2cor_conf["SEN2COR_AUX_DIR"]}:/home/lib/python2.7/site-packages/sen2cor/aux_data \
                        -v {sen2cor_conf["SEN2COR_CONFIG_DIR"]}:/root/sen2cor/2.8 \
                        {sen2cor_conf["SEN2COR_DOCKER_IMAGE"]} {entry}'''
                    env['OUTDIR'] = str(Path(tmp) / 'output')
                else:
                    output_path.mkdir(exist_ok=True, parents=True)

                    lasrc_conf = Config.LASRC_CONFIG

                    cmd = f'''docker run --rm -i \
                        -v $INDIR:/mnt/input-dir \
                        -v $OUTDIR:/mnt/output-dir \
                        --env INDIR=/mnt/input-dir \
                        --env OUTDIR=/mnt/output-dir \
                        -v {lasrc_conf["LASRC_AUX_DIR"]}:/mnt/lasrc-aux:ro \
                        -v {lasrc_conf["LEDAPS_AUX_DIR"]}:/mnt/ledaps-aux:ro \
                        {lasrc_conf["LASRC_DOCKER_IMAGE"]} {entry}'''

                logging.debug(cmd)

                # subprocess
                process = subprocess.Popen(cmd, shell=True, env=env, stdin=subprocess.PIPE)
                process.wait()

                assert process.returncode == 0

                # TODO: We should be able to get output name from execution
                if processor_name.lower() == 'sen2cor':
                    # Since sen2cor creates an custom directory name (based in scene_id) and changing processing date
                    # we create it inside "output" folder. After that, get first entry of that directory
                    output_tmp = list(Path(env['OUTDIR']).iterdir())[0]

                    output_path = output_path.parent / output_tmp.name

                    if execution.activity.args.get('file'):
                        last_processed_file = execution.activity.args['file']

                        if last_processed_file and os.path.exists(last_processed_file) and \
                                last_processed_file.endswith('.SAFE'):
                            # TODO: validate scene id (without processing_date)
                            if len(os.listdir(last_processed_file)) < 9:
                                shutil.rmtree(last_processed_file, ignore_errors=True)

                    shutil.move(output_tmp, output_path)

                refresh_execution_args(execution, activity, file=str(output_path))
        else:
            raise RuntimeError(f'Processor not supported. Check collection {collection.name} metadata processors')
    except Exception as e:
        logging.error(f'Error in correction {scene_id} - {str(e)}', exc_info=True)
        raise e

    return activity


@current_app.task(queue='publish')
def publish(activity: dict, collection_id=None, **kwargs):
    execution = execution_from_collection(activity, collection_id=collection_id, activity_type=publish.__name__)

    collection = execution.activity.collection

    scene_id = activity['sceneid']

    logging.info(f'Starting Publish Task for {collection.name}(id={collection.id}, scene_id={scene_id})')

    try:
        data_collection = get_provider_collection_from_activity(activity)

        file = activity['args'].get('file') or activity['args'].get('compressed_file')

        publish_collection(scene_id, data_collection, collection, file, cloud_cover=activity['args'].get('cloud'))

        if file:
            refresh_execution_args(execution, activity, file=str(file))
    except RuntimeError as e:
        logging.error(f'Error in publish {scene_id} - {str(e)}', exc_info=True)
        raise

    return activity


@current_app.task(queue='post')
def post(activity: dict, collection_id=None, **kwargs):
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


@current_app.task(queue='harmonization')
def harmonization(activity: dict, collection_id=None, **kwargs):
    execution = execution_from_collection(activity, collection_id=collection_id, activity_type=harmonization.__name__)

    collection = execution.activity.collection

    from sensor_harm import sentinel_harmonize
    from sensor_harm import landsat_harmonize

    with TemporaryDirectory(prefix='harmonization', suffix=activity['sceneid']) as tmp:
        data_collection = get_provider_collection_from_activity(activity)

        data_collection.path(collection)

        target_dir = str(data_collection.path(collection))

        target_tmp_dir = Path(tmp) / 'target'

        target_tmp_dir.mkdir(exist_ok=True, parents=True)

        if activity['sceneid'].startswith('S2'):
            shutil.unpack_archive(activity['args']['compressed_file'], tmp)

            entry = activity['sceneid']
            entries = list(Path(tmp).glob('*.SAFE'))

            if len(entries) == 1 and entries[0].suffix == '.SAFE':
                entry = entries[0].name

            l1 = Path(tmp) / entry

            sentinel_harmonize(l1, activity['args']['file'], target_tmp_dir, apply_bandpass=True)
        else:
            product_version = int(data_collection.parser.satellite())
            sat_sensor = '{}{}'.format(data_collection.parser.source()[:2], product_version)

            landsat_harmonize(sat_sensor, activity['args']['file'], str(target_tmp_dir))

        reflectance_dir = Path(activity['args']['file'])

        glob = list(reflectance_dir.glob('**/*_Fmask4.tif'))

        fmask = glob[0]

        shutil.copy(str(fmask), target_tmp_dir)

        Path(target_dir).mkdir(exist_ok=True, parents=True)

        for entry in Path(target_tmp_dir).iterdir():
            shutil.move(str(entry), target_dir)

    activity['args']['file'] = target_dir

    return activity
