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

import logging
import subprocess
from pathlib import Path
from typing import Optional

from ..config import Config


def sen2cor(scene_id: str, input_dir: str, output_dir: str,
            docker_container_work_dir: list, version: Optional[str] = None, **env):
    """Execute Sen2Cor data processor using Docker images.

    Note:
        Make sure you have exported the variables ``SEN2COR_AUX_DIR``, ``SEN2COR_DOCKER_IMAGE``,
        and ``SEN2COR_DIR`` properly.

    This method calls the processor ``Sen2Cor`` and generate the ``Surface Reflectance``
    products. Once the required variables is set, it tries to execute Sen2Cor from the
    given versions: '2.10.0', '2.8.0', '2.5.5'.

    Args:
        scene_id (str): The Scene Identifier (Item id)
        input_dir (str): Base input directory of scene id.
        output_dir (str): Path where Surface reflectance product will be generated.
        docker_container_work_dir (str): Base directory list of workdir for docker.
        version (str): Sen2Cor version to execute.
            Remember that you must exist the version in docker registry. Defaults is ``None``, which
            automatically tries the versions '2.10.0', '2.8.0', '2.5.5', respectively.
    Keyword Args:
        any: Custom Environment variables, use Python spread kwargs.
    """
    if version is not None:
        version_minor = '.'.join(version.split('.')[:-1])
        args = [
            'docker', 'run', '--rm', '-i',
            '--name', scene_id,
            '-v', f'{input_dir}:{Config.SEN2COR_CONFIG["SEN2COR_CONTAINER_INPUT_DIR"]}',
            '-v', f'{output_dir}:{Config.SEN2COR_CONFIG["SEN2COR_CONTAINER_OUTPUT_DIR"]}',
            '-v', f'{Config.SEN2COR_CONFIG["SEN2COR_DIR"]}/CCI4SEN2COR:/mnt/aux_data',
            '-v', f'{Config.SEN2COR_CONFIG["SEN2COR_DIR"]}/{version_minor}/cfg/L2A_GIPP.xml:/opt/sen2cor/{version}/cfg/L2A_GIPP.xml',
            *docker_container_work_dir,
            f'{Config.SEN2COR_CONFIG["SEN2COR_DOCKER_IMAGE"]}:{version}',
            f'{scene_id}.SAFE'
        ]

        logging.info(f'Using Sen2Cor {version}')

        process = subprocess.Popen(args, env=env, stdin=subprocess.PIPE)
        process.wait()

        if process.returncode != 0:
            raise RuntimeError(f'Could not execute Sen2Cor using {version}')

        output_tmp = list(Path(output_dir).iterdir())[0]

        output_path = Path(output_dir) / output_tmp.name

        return output_path

    def _safe_execute(*args, **kwargs):
        try:
            return sen2cor(*args, **kwargs), None
        except RuntimeError as e:
            return None, e

    versions_supported = Config.SEN2COR_CONFIG['SEN2COR_VERSIONS_SUPPORTED'].split(';')

    err = None
    for version in versions_supported:
        out, err = _safe_execute(scene_id, input_dir, output_dir, docker_container_work_dir, version=version, **env)
        if out:
            return out

    raise RuntimeError(f'Could not execute Sen2Cor using {versions_supported} - {err}')
