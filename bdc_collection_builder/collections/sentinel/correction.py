#
# This file is part of Brazil Data Cube Collection Builder.
# Copyright (C) 2019-2020 INPE.
#
# Brazil Data Cube Collection Builder is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.
#

"""Describe Sentinel 2 atmosphere correction."""

# Python Native
import logging
import os
import re
import shutil
import time
from json import loads as json_parser

# 3rdparty
from requests import get as resource_get

# Builder
from bdc_collection_builder.config import Config


def search_recent_sen2cor280(safeL2Afull):
    """Search recent .SAFE folder from Sentinel.

    Args:
        safeL2Afull - Path to the folder where .SAFE files generated.
    """
    safe = safeL2Afull.replace(os.path.basename(safeL2Afull).split('_')[3], 'N9999')
    safe_pattern = '_'.join(os.path.basename(safe).split('_')[0:-1])
    dirname = os.path.dirname(safeL2Afull)
    dirs_L2 = [os.path.join(dirname,d) for d in os.listdir(dirname) if re.match('^{}.*SAFE$'.format(safe_pattern), d)]
    return dirs_L2


def correction_sen2cor255(scene):
    """Dispatch sen2cor 2.5.5 execution."""
    safeL2Afull = scene['file'].replace('MSIL1C','MSIL2A')
    # TODO: check if file exists and validate SAFE
    valid = False
    if os.path.exists(safeL2Afull) and valid == True:
        return safeL2Afull
    if not os.path.exists(safeL2Afull) or valid == False:
        # Send scene to the sen2cor service

        req = resource_get('{}/sen2cor'.format(Config.SEN2COR_URL), params=scene)
        # Ensure the request has been successfully
        assert req.status_code == 200

        result = json_parser(req.content)

        if result and result.get('status') == 'ERROR':
            if os.path.exists(safeL2Afull):
                shutil.rmtree(safeL2Afull, ignore_errors=True)
            raise RuntimeError('Error in sen2cor execution')

    return safeL2Afull


def correction_sen2cor280(scene):
    """Dispatch sen2cor 2.8.0 execution."""
    safeL2Afull = scene['file'].replace('MSIL1C', 'MSIL2A')
    dirs_L2 = search_recent_sen2cor280(safeL2Afull)
    if len(dirs_L2) >= 1:
        for i in range(len(dirs_L2) - 1):
            shutil.rmtree(dirs_L2[i], ignore_errors=True)
        #TODO: Validate SAFE os.path.join(dirname, dirs_L2[0])
        valid = False
        if valid == True:
            logging.info('sen2cor skipped')
            # scene['file'] = dirs_L2[0]
            return dirs_L2[-1]
        else:
            shutil.rmtree(dirs_L2[-1], ignore_errors=True)

    # Send scene to the sen2cor service
    req = resource_get('{}/sen2cor'.format(Config.SEN2COR_URL), params=scene)
    # Ensure the request has been successfully
    assert req.status_code == 200

    result = json_parser(req.content)

    if result and result.get('status') == 'ERROR':
        raise RuntimeError('Error in sen2cor execution')

    dirs_L2 = search_recent_sen2cor280(safeL2Afull)

    return dirs_L2[-1]
