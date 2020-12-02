#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Module for Data Synchronization on AWS Buckets."""

# Python Native
import logging
import shutil
# 3rdparty
import boto3
# BDC Scripts
from bdc_collection_builder.config import Config
from pathlib import Path


def _s3_bucket_instance(bucket: str):
    s3 = boto3.resource(
        's3', region_name=Config.AWS_REGION_NAME,
        aws_access_key_id=Config.AWS_ACCESS_KEY_ID, aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
    )

    bucket = s3.Bucket(bucket)

    return s3, bucket


class DataSynchronizer:
    """Class for synchronize a folder with AWS Buckets."""

    def __init__(self, file_path: str, bucket: str = Config.COLLECTION_BUILDER_SYNC_BUCKET):
        """Build instance object."""
        self.file_path = Path(file_path)
        self.bucket = bucket
        self.prefix = Path(Config.DATA_DIR) / 'Repository/Archive'

    def __instance(self):
        s3 = boto3.resource(
            's3', region_name=Config.AWS_REGION_NAME,
            aws_access_key_id=Config.AWS_ACCESS_KEY_ID, aws_secret_access_key=Config.AWS_SECRET_ACCESS_KEY
        )

        bucket = s3.Bucket(self.bucket)

        return s3, bucket

    def check_data(self):
        """Try to check file availability both in local and AWS.

        Notes:
            To activate this feature, make sure to set `COLLECTION_BUILDER_SYNC=True`.

        This method aim's to solve the problem to work with multiple workers on AWS.
        Since ``Collection-Builder`` deal with multiprocessing tasks, it requires an shared volume where
        the workers can manipulate the data and then pass the result to the other node to continue process stream.

        On Amazon Web Service environment, this feature is only available in North American Servers, however, the
        Sentinel-2 data server is located in Frankfurt.
        To do that, we have created this feature to the workers store the result temporally data in
        the Amazon Simple Storage Service (S3).

        Warning:
            Currently, ``Collection-Builder`` is not fully supporting auto-removal data from AWS on Exceptions.

        Args:
            file_path - Path to file / folder to require from AWS.
            bucket - Bucket to check for. Default is ``Config.COLLECTION_BUILDER_SYNC_BUCKET``
        """
        expected_file_path = Path(self.file_path)

        # When required file not in disk, seek in the bucket
        if not expected_file_path.exists():
            logging.info(f'File {str(self.file_path)} is not available here. Checking in bucket {self.bucket}')

            _, bucket = _s3_bucket_instance(self.bucket)

            relative_path = expected_file_path.relative_to(self.prefix)

            for blob in bucket.objects.filter(Prefix=str(relative_path)):
                destination = self.prefix / blob.key

                destination.parent.mkdir(exist_ok=True, parents=True)

                bucket.download_file(blob.key, str(destination))

    @staticmethod
    def is_remote_sync_configured():
        """Check if DataSynchronizer is fully supported."""
        return Config.COLLECTION_BUILDER_SYNC

    def remove_data(self, raise_error=False):
        """Try to remove any folder for both local and AWS buckets."""
        path = Path(self.file_path)

        if path.exists():
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
            elif path.is_file():
                path.unlink()

        _, bucket = _s3_bucket_instance(self.bucket)

        relative_path = path.relative_to(self.prefix)

        try:
            bucket.delete_objects(
                Delete=dict(
                    Objects=[
                        dict(Key=str(relative_path))
                    ]
                )
            )
            logging.info(f'Entry {str(relative_path)} removed from {self.bucket}')
        except Exception as e:
            logging.error(f'Cannot remove {str(relative_path)} - {str(e)}')
            if raise_error:
                raise e

    def sync_data(self, file_path: str = None, bucket: str = None, auto_remove=False):
        """Synchronize data with buckets."""
        expected_file_path = Path(file_path or self.file_path)

        if not expected_file_path.exists():
            raise RuntimeError(f'File {str(expected_file_path)} does not exists.')

        _bucket = bucket or self.bucket
        logging.info(f'Uploading {str(self.file_path)} to bucket {_bucket}')

        _, bucket = _s3_bucket_instance(_bucket)

        relative_path = expected_file_path.relative_to(self.prefix)

        if expected_file_path.is_file():
            bucket.upload_file(str(expected_file_path), str(relative_path))

            if auto_remove:
                expected_file_path.unlink()
        else:
            for path in expected_file_path.iterdir():
                if path.is_dir():
                    continue

                item_relative = path.relative_to(self.prefix)

                bucket.upload_file(str(path), str(item_relative))

                if auto_remove:
                    path.unlink()
