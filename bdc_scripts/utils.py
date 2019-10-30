from os import path as resource_path
from zlib import error as zlib_error
from zipfile import BadZipfile, ZipFile


def extractall(file):
    formatted_filename = file.replace('.zip', '.SAFE')

    if not resource_path.exists(formatted_filename):
        archive = ZipFile(file, 'r')
        archive.extractall(resource_path.dirname(file))
        archive.close()


def is_valid(file):
    try:
        archive = ZipFile(file, 'r')
        try:
            corrupt = archive.testzip()
        except zlib_error:
            corrupt = True
        archive.close()
    except BadZipfile:
        corrupt = True

    return not corrupt
