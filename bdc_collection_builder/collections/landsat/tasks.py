#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Describe Celery task handling for Landsat products."""

# Python Native
import logging
import os
import shutil
import subprocess
from datetime import datetime
from tempfile import TemporaryDirectory

# 3rdparty
from bdc_catalog.models import Collection
from botocore.exceptions import EndpointConnectionError
from sqlalchemy.exc import InvalidRequestError
from urllib3.exceptions import NewConnectionError, MaxRetryError

# Builder
from ...celery import celery_app
from ...config import Config
from ...db import db_aws
from ..base_task import RadcorTask
from ..utils import post_processing, remove_file, upload_file
from .download import download_landsat_images
from .google import download_from_google
from .harmonization import landsat_harmonize
from .publish import publish
from .utils import LandsatProduct


def is_valid_tar_gz(file_path: str):
    """Check tar file integrity."""
    try:
        retcode = subprocess.call(['gunzip', '-t', file_path])
        return retcode == 0
    except BaseException:
        return False


class LandsatTask(RadcorTask):
    """Define abstraction of Landsat-8 - DN and SR products."""

    def get_tile_id(self, scene_id, **kwargs):
        """Retrieve tile from sceneid."""
        fragments = scene_id.split('_')
        return fragments[2]

    def get_tile_date(self, scene_id, **kwargs):
        """Retrieve tile date from sceneid."""
        fragments = scene_id.split('_')

        return datetime.strptime(fragments[3], '%Y%m%d')

    def download(self, scene, **kwargs):
        """Perform download landsat image from USGS.

        Args:
            scene (dict) - Scene containing activity

        Returns:
            dict Scene with landsat compressed file
        """
        # Create/Update activity
        activity_history = self.create_execution(scene)

        collection: Collection = activity_history.activity.collection

        try:
            scene_id = scene['sceneid']
            # Get Landsat collection handler
            landsat_scene = LandsatProduct(scene_id, collection=collection)

            activity_args = scene.get('args', {})

            # Output product dir
            productdir = landsat_scene.compressed_file().parent

            productdir.mkdir(parents=True, exist_ok=True)

            digital_number_file = landsat_scene.compressed_file()

            valid = False

            # When file exists, check persistence
            if digital_number_file.exists() and digital_number_file.is_file():
                logging.info('File {} downloaded. Checking file integrity...'.format(str(digital_number_file)))
                # Check Landsat 8 tar gz is valid
                valid = is_valid_tar_gz(str(digital_number_file))

                file = str(digital_number_file)

            if not valid:
                # Ensure file is removed since it may be corrupted
                remove_file(str(digital_number_file))

                # Create temporary directory and move to right place after downloaded.
                with TemporaryDirectory(prefix=scene_id) as tmp:
                    try:
                        # Download from google
                        logging.info('Download Landsat {} -> e={} v={} from Google...'.format(
                            scene_id, digital_number_file.exists(), valid)
                        )
                        file, link = download_from_google(scene_id, collection, str(tmp))
                        activity_args['provider'] = link
                    except BaseException:
                        logging.info('Download Landsat {} from USGS...'.format(scene_id))
                        file = download_landsat_images(activity_args['link'], tmp)
                        activity_args['provider'] = activity_args['link']

                    # Move file downloaded in temporary directory to the right place
                    shutil.move(file, str(digital_number_file))
                    file = str(digital_number_file)
            else:
                logging.warning('File {} is valid. Skipping'.format(str(digital_number_file)))

            activity_args['compressed_file'] = str(file)
        except BaseException as e:
            logging.error('An error occurred during task execution - {}'.format(activity_history.activity_id),
                          exc_info=True)

            raise e

        scene['args'] = activity_args

        # Create new activity 'correctionLC8' to continue task chain
        scene['activity_type'] = 'correctionLC8'

        return scene

    def publish(self, scene, collection_id=None, **kwargs):
        """Publish and persist collection on database.

        Args:
            scene - Serialized Activity
        """
        scene['activity_type'] = 'publishLC8'

        if collection_id:
            scene['collection_id'] = collection_id

        # Create/Update activity
        activity_history = self.create_execution(scene)

        args = activity_history.activity.args.copy()
        args.update(kwargs)

        activity_history.activity.args = args
        activity_history.save()

        try:
            item = self.get_collection_item(activity_history.activity)
            assets = publish(item, activity_history.activity, **args)
        except InvalidRequestError as e:
            # Error related with Transaction on AWS
            # TODO: Is it occurs on local instance?
            logging.error("Transaction Error on activity - {}".format(activity_history.activity_id), exc_info=True)

            db_aws.session.rollback()

            raise e

        except BaseException as e:
            logging.error("An error occurred during task execution - {}".format(activity_history.activity_id),
                          exc_info=True)

            raise e

        scene['activity_type'] = 'uploadLC8'
        scene['args']['assets'] = assets

        return scene

    def upload(self, scene):
        """Upload collection to AWS.

        Make sure to set `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
        `AWS_REGION_NAME` defined in `bdc_collection_builder.config.Config`.

        Args:
            scene - Serialized Activity
        """
        scene['activity_type'] = 'uploadLC8'

        # Create/Update activity
        self.create_execution(scene)

        assets = scene['args']['assets']

        for entry in assets.values():
            file_without_prefix = entry['asset'].replace('{}/'.format(Config.AWS_BUCKET_NAME), '')
            upload_file(entry['file'], Config.AWS_BUCKET_NAME, file_without_prefix)

    @staticmethod
    def espa_done(scene: LandsatProduct):
        """Check espa-science has executed successfully."""
        fs = scene.get_files()

        return len(fs) > 0

    def correction(self, scene, collection_id=None, **kwargs):
        """Apply atmospheric correction on collection.

        Args:
            scene - Serialized Activity
        """
        import subprocess
        import tarfile

        scene['activity_type'] = 'correctionLC8'
        scene_id = scene['sceneid']

        if collection_id:
            scene['collection_id'] = collection_id

        # Create/Update activity
        execution = self.create_execution(scene)

        collection: Collection = execution.activity.collection

        # Get Resolver for Landsat scene level 2
        landsat_scene = LandsatProduct(scene_id, collection=collection)

        try:
            params = dict(
                app=scene['activity_type'],
                sceneid=scene['sceneid'],
                file=scene['args']['compressed_file']
            )

            output_path = landsat_scene.path()
            output_path.mkdir(exist_ok=True, parents=True)

            with TemporaryDirectory(prefix='correction', suffix=scene_id) as tmp:
                with tarfile.open(scene['args']['compressed_file']) as compressed_file:
                    # Extracting to temp directory
                    compressed_file.extractall(tmp)

                input_dir = str(tmp)  # landsat_scene_level_1.compressed_file().parent
                cmd = 'run_lasrc_ledaps_fmask.sh {}'.format(scene_id)

                logging.warning('cmd {}'.format(cmd))

                env = dict(**os.environ, INDIR=str(input_dir), OUTDIR=str(output_path))
                process = subprocess.Popen(cmd, shell=True, env=env, stdin=subprocess.PIPE)
                process.wait()

                assert process.returncode == 0

            pathrow = landsat_scene.tile_id()

            params['pathrow'] = pathrow

            # Product dir
            productdir = landsat_scene.path()

            logging.info('Checking for the ESPA generated files in {}'.format(productdir))

            if not LandsatTask.espa_done(landsat_scene):
                raise RuntimeError('Error in atmospheric correction')

            scene['args']['file'] = str(productdir)

        except BaseException as e:
            logging.error('Error at correction Landsat {}, id={} - {}'.format(scene_id, execution.activity_id, str(e)))

            raise e

        scene['activity_type'] = 'publishLC8'

        return scene

    def harmonize(self, scene):
        """Apply Harmonization on Landsat collection.

        Args:
            scene - Serialized Activity
        """
        # Set Collection Level 3 - BDC
        scene['args']['level'] = 3

        # Create/Update activity
        activity_history = self.create_execution(scene)

        collection: Collection = activity_history.activity.collection

        landsat_scene = LandsatProduct(scene['sceneid'], collection=collection)

        # Set Collection to the Landsat NBAR (Nadir BRDF Adjusted Reflectance)
        scene['collection_id'] = landsat_scene.id
        scene['activity_type'] = 'harmonizeLC8'

        logging.debug('Starting Harmonization Landsat...')

        activity_history.activity.activity_type = 'harmonizeLC8'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            # Get ESPA output dir
            harmonized_dir = landsat_harmonize(self.get_collection_item(activity_history.activity), activity_history.activity)
            scene['args']['file'] = harmonized_dir

        except BaseException as e:
            logging.error('Error at Harmonize Landsat {}'.format(e))

            raise e

        scene['activity_type'] = 'publishLC8'

        return scene

    def post_publish(self, scene):
        logging.info(f'Applying post-processing for {scene["sceneid"]}')
        collection = Collection.query().filter(Collection.id == scene['collection_id']).first()

        assets = scene['args']['assets']

        for entry in assets.values():
            if entry['file'].endswith('Fmask4.tif'):
                post_processing(entry['file'], collection, assets)

        return scene


@celery_app.task(base=LandsatTask,
                 queue='download',
                 max_retries=72,
                 autoretry_for=(NewConnectionError, MaxRetryError),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def download_landsat(scene, **kwargs):
    """Represent a celery task definition for handling Landsat-8 Download files.

    This celery tasks listen only for queues 'download'.

    It also retries following errors occurs:
        - NewConnectionError, MaxRetryError Internet Connection Problem

    Args:
        scene (dict): Radcor Activity

    Returns:
        Returns processed activity
    """
    return download_landsat.download(scene, **kwargs)


@celery_app.task(base=LandsatTask, queue='atm-correction')
def atm_correction_landsat(scene, **kwargs):
    """Represent a celery task definition for handling Landsat Atmospheric correction - sen2cor.

    This celery tasks listen only for queues 'atm-correction'.

    Args:
        scene (dict): Radcor Activity with "correctionLC8" app context

    Returns:
        Returns processed activity
    """
    return atm_correction_landsat.correction(scene, **kwargs)


@celery_app.task(base=LandsatTask,
                 queue='publish',
                 max_retries=3,
                 autoretry_for=(InvalidRequestError,),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def publish_landsat(scene, **kwargs):
    """Represent a celery task definition for handling Landsat Publish TIFF files generation.

    This celery tasks listen only for queues 'publish'.

    It also retries following errors occurs:
        - InvalidRequestError Error related with transaction error on multiple access to database.

    Args:
        scene (dict): Radcor Activity with "publishLC8" app context

    Returns:
        Returns processed activity
    """
    return publish_landsat.publish(scene, **kwargs)


@celery_app.task(base=LandsatTask,
                 queue='upload',
                 max_retries=3,
                 auto_retry=(EndpointConnectionError, NewConnectionError,),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def upload_landsat(scene, **kwargs):
    """Represent a celery task definition for handling Landsat8 Upload TIFF to AWS.

    This celery tasks listen only for queues 'uploadLC8'.

    Args:
        scene (dict): Radcor Activity with "uploadLC8" app context
    """
    upload_landsat.upload(scene, **kwargs)


@celery_app.task(base=LandsatTask, queue='harmonization')
def harmonization_landsat(scene):
    """Represent a celery task definition for harmonizing Landsat8.

    This celery tasks listen only for queues 'harmonizeLC8'.

    Args:
        scene (dict): Radcor Activity with "harmonizeLC8" app context
    """
    return harmonization_landsat.harmonize(scene)


@celery_app.task(base=LandsatTask, queue='post-processing')
def apply_post_processing(scene):
    return apply_post_processing.post_publish(scene)
