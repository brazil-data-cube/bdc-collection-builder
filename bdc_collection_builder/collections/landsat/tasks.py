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
import subprocess
from datetime import datetime
from pathlib import Path
# 3rdparty
from botocore.exceptions import EndpointConnectionError
from glob import glob as resource_glob
from requests import get as resource_get
from sqlalchemy.exc import InvalidRequestError
from urllib3.exceptions import NewConnectionError, MaxRetryError
# Builder
from bdc_collection_builder.celery import celery_app
from bdc_collection_builder.config import Config
from bdc_collection_builder.core.utils import upload_file
from bdc_collection_builder.collections.base_task import RadcorTask
from bdc_collection_builder.collections.landsat.download import download_landsat_images, download_from_aws
from bdc_collection_builder.collections.landsat.harmonization import landsat_harmonize
from bdc_collection_builder.collections.landsat.publish import publish
from bdc_collection_builder.collections.utils import upload_file
from bdc_collection_builder.db import db_aws


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

    def download(self, scene):
        """Perform download landsat image from USGS.

        Args:
            scene (dict) - Scene containing activity

        Returns:
            dict Scene with landsat compressed file
        """
        # Create/Update activity
        activity_history = self.create_execution(scene)

        try:
            scene_id = scene['sceneid']
            yyyymm = self.get_tile_date(scene_id).strftime('%Y-%m')
            activity_args = scene.get('args', {})

            collection_item = self.get_collection_item(activity_history.activity)

            # Output product dir
            productdir = os.path.join(activity_args.get('file'), '{}/{}'.format(yyyymm, self.get_tile_id(scene_id)))

            os.makedirs(productdir, exist_ok=True)

            digital_number_file = Path(productdir) / '{}.tar.gz'.format(scene_id)

            valid = False

            # When file exists, check persistence
            if digital_number_file.exists() and digital_number_file.is_file():
                logging.info('File {} downloaded. Checking file integrity...'.format(str(digital_number_file)))
                # Check Landsat 8 tar gz is valid
                valid = is_valid_tar_gz(str(digital_number_file))

                file = str(digital_number_file)

            if not valid:
                try:
                    logging.info('Download Lansat {} -> e={} v={} from AWS...'.format(scene_id, digital_number_file.exists(), valid))
                    digital_number_dir = os.path.join(Config.DATA_DIR, 'Repository/Archive/{}/{}/{}'.format(
                        scene['collection_id'],
                        yyyymm,
                        self.get_tile_id(scene_id)
                    ))

                    file = download_from_aws(scene_id, digital_number_dir, productdir)
                except BaseException:
                    logging.warning('Could not download {} from AWS. Using USGS...'.format(scene_id))

                    file = download_landsat_images(activity_args['link'], productdir)
            else:
                logging.warning('File {} is valid. Skipping'.format(str(digital_number_file)))

            collection_item.compressed_file = file.replace(Config.DATA_DIR, '')

            cloud = activity_args.get('cloud')

            if cloud:
                collection_item.cloud_cover = cloud

            activity_args['file'] = file
        except BaseException as e:
            logging.error('An error occurred during task execution - {}'.format(activity_history.activity_id),
                          exc_info=True)

            raise e

        collection_item.save()

        scene['args'] = activity_args

        # Create new activity 'correctionLC8' to continue task chain
        scene['activity_type'] = 'correctionLC8'

        return scene

    def publish(self, scene):
        """Publish and persist collection on database.

        Args:
            scene - Serialized Activity
        """
        scene['activity_type'] = 'publishLC8'

        # Create/Update activity
        activity_history = self.create_execution(scene)

        try:
            assets = publish(self.get_collection_item(activity_history.activity), activity_history.activity)
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
    def espa_done(productdir, pathrow, date):
        """Check espa-science has executed successfully."""
        template = os.path.join(productdir, 'LC08_*_{}_{}_*.tif'.format(pathrow, date))

        fs = resource_glob(template)

        return len(fs) > 0

    def correction(self, scene):
        """Apply atmospheric correction on collection.

        Args:
            scene - Serialized Activity
        """
        scene['collection_id'] = 'LC8SR'
        scene['activity_type'] = 'correctionLC8'
        scene_id = scene['sceneid']

        # Create/Update activity
        execution = self.create_execution(scene)

        try:
            params = dict(
                app=scene['activity_type'],
                sceneid=scene['sceneid'],
                file=scene['args']['file']
            )

            pathrow = self.get_tile_id(scene_id)
            tile_date = self.get_tile_date(scene_id)
            yyyymm = tile_date.strftime('%Y-%m')
            date = tile_date.strftime('%Y%m%d')

            params['pathrow'] = pathrow

            # Send scene to the ESPA service
            req = resource_get('{}/espa'.format(Config.ESPA_URL), params=params)
            # Ensure the request has been successfully
            assert req.status_code == 200

            result = req.json()

            if result and result.get('status') == 'ERROR':
                raise RuntimeError('Error in espa-science execution - {}'.format(scene_id))

            # Product dir
            productdir = os.path.join(Config.DATA_DIR, 'Repository/Archive/{}/{}/{}'.format(scene['collection_id'], yyyymm, pathrow))

            logging.info('Checking for the ESPA generated files in {}'.format(productdir))

            if not LandsatTask.espa_done(productdir, pathrow, date):
                raise RuntimeError('Error in atmospheric correction')

            scene['args']['file'] = productdir

        except BaseException as e:
            logging.error('Error at correction Landsat {}, id={} - {}'.format(scene_id, execution.activity_id, str(e)))

            raise e

        scene['activity_type'] = 'publishLC8'

        return scene

    def harmonize(self, scene):
        """Apply Harmonization on collection.

        Args:
            scene - Serialized Activity
        """
        # Set Collection to the Landsat NBAR (Nadir BRDF Adjusted Reflectance)
        scene['collection_id'] = 'LC8NBAR'
        scene['activity_type'] = 'harmonizeLC8'

        # Create/Update activity
        activity_history = self.create_execution(scene)

        logging.debug('Starting Harmonization Landsat...')
        logging.info('L8TASKS Harmonize Starting Harmonization Landsat...') #TODO REMOVE

        activity_history.activity.activity_type = 'harmonizeLC8'
        activity_history.start = datetime.utcnow()
        activity_history.save()

        try:
            # Get ESPA output dir
            harmonized_dir = landsat_harmonize(self.get_collection_item(activity_history.activity), activity_history.activity)
            scene['args']['file'] = harmonized_dir

        except BaseException as e:
            logging.error('Error at Harmonize Landsat', e)

            raise e

        scene['activity_type'] = 'publishLC8'

        return scene


@celery_app.task(base=LandsatTask,
                 queue='download',
                 max_retries=72,
                 autoretry_for=(NewConnectionError, MaxRetryError),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def download_landsat(scene):
    """Represent a celery task definition for handling Landsat-8 Download files.

    This celery tasks listen only for queues 'download'.

    It also retries following errors occurs:
        - NewConnectionError, MaxRetryError Internet Connection Problem

    Args:
        scene (dict): Radcor Activity

    Returns:
        Returns processed activity
    """
    return download_landsat.download(scene)


@celery_app.task(base=LandsatTask, queue='atm-correction')
def atm_correction_landsat(scene):
    """Represent a celery task definition for handling Landsat Atmospheric correction - sen2cor.

    This celery tasks listen only for queues 'atm-correction'.

    Args:
        scene (dict): Radcor Activity with "correctionLC8" app context

    Returns:
        Returns processed activity
    """
    return atm_correction_landsat.correction(scene)


@celery_app.task(base=LandsatTask,
                 queue='publish',
                 max_retries=3,
                 autoretry_for=(InvalidRequestError,),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def publish_landsat(scene):
    """Represent a celery task definition for handling Landsat Publish TIFF files generation.

    This celery tasks listen only for queues 'publish'.

    It also retries following errors occurs:
        - InvalidRequestError Error related with transaction error on multiple access to database.

    Args:
        scene (dict): Radcor Activity with "publishLC8" app context

    Returns:
        Returns processed activity
    """
    return publish_landsat.publish(scene)


@celery_app.task(base=LandsatTask,
                 queue='upload',
                 max_retries=3,
                 auto_retry=(EndpointConnectionError, NewConnectionError,),
                 default_retry_delay=Config.TASK_RETRY_DELAY)
def upload_landsat(scene):
    """Represent a celery task definition for handling Landsat8 Upload TIFF to AWS.

    This celery tasks listen only for queues 'uploadLC8'.

    Args:
        scene (dict): Radcor Activity with "uploadLC8" app context
    """
    upload_landsat.upload(scene)
