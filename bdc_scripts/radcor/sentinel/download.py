import logging
import os
import requests


def download_sentinel_images(link, file_path, user):
    """
    Download sentinel image from Copernicus (compressed data)

    Args:
        link (str) - Sentinel Image Link
        file_path (str) - Path to save download file
        user (AtomicUser) - User credential
    """
    try:
        response = requests.get(link, auth=(user.username, user.password), timeout = 90, stream=True)
    except requests.exceptions.ConnectionError as e:
        logging.error('Connection error during Sentinel Download')
        raise e

    if response.status_code >= 400:
        raise requests.exceptions.HTTPError('Invalid sentinel request {}'.format(response.status_code))

    size = int(response.headers['Content-Length'].strip())

    logging.info('Downloading image {}, user {}, size {} MB'.format(link, user, int(size / 1024 / 1024)))

    dirname = os.path.dirname(file_path)

    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # May throw exception for read-only directory
    stream = open(file_path, 'wb')

    # Read chunks of 2048 bytes
    chunk_size = 2048

    for chunk in response.iter_content(chunk_size):
        stream.write(chunk)

    stream.close()