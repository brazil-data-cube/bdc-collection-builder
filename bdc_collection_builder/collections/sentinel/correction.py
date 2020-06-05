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
import subprocess
from datetime import datetime
from pathlib import Path
from shutil import rmtree
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

    fragments = Path(safeL2Afull).name.split('_')
    fragments[3] = 'N9999'

    processing_date = datetime.strptime(fragments[-1].replace('.SAFE', ''), '%Y%m%dT%H%M%S')
    today = datetime.utcnow().timetuple()

    processing_date = processing_date.replace(year=today.tm_year, month=today.tm_mon, day=today.tm_mday)

    output_dir = '_'.join(fragments[:-1])
    sensing_date = datetime.strptime(fragments[2], '%Y%m%dT%H%M%S')
    output_dir = Config.DATA_DIR / Path('Repository/Archive/S2_MSI/{}/{}_{}.SAFE'.format(sensing_date.strftime('%Y-%m'), output_dir,
                                                                       processing_date.strftime('%Y%m%dT%H%M%S')))

    os.makedirs(str(output_dir), exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info('Using outputdir {}'.format(str(output_dir)))
    scene['output_dir'] = str(output_dir)

    # Send scene to the sen2cor service
    req = resource_get('{}/sen2cor'.format(Config.SEN2COR_URL), params=scene)

    result = json_parser(req.content)

    if req.status_code != 200 and result and result.get('status') == 'ERROR':
        rmtree(str(output_dir))
        raise RuntimeError('Error in sen2cor execution')

    return str(output_dir)


def correction_laSRC(input_dir: str, output_dir: str) -> str:
    scene_id_safe = Path(input_dir).name

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # "input_dir" usually already points to .SAFE. We need the dir name
    base_input_dir = str(Path(input_dir).parent)

    cmd = 'run_lasrc_ledaps_fmask.sh {}'.format(scene_id_safe)

    logging.warning('cmd {}'.format(cmd))

    env = dict(**os.environ, INDIR=base_input_dir, OUTDIR=str(output_dir))

    process = subprocess.Popen(cmd, shell=True, env=env)
    process.wait()

    if process.returncode != 0:
        raise RuntimeError('Error in LaSRC generation')

    return output_dir
