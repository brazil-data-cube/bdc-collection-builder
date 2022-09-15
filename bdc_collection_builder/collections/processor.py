import logging
import subprocess
from pathlib import Path
from typing import Optional

from ..config import Config


def sen2cor(scene_id: str, input_dir: str, output_dir: str,
            docker_container_work_dir: list, version: Optional[str] = None, **env):
    if version is not None:
        version_minor = '.'.join(version.split('.')[:-1])
        args = [
            'docker', 'run', '--rm', '-i',
            '--name', scene_id,
            '-v', f'{input_dir}:/mnt/input-dir',
            '-v', f'{output_dir}:/mnt/output-dir',
            '-v', f'{Config.SEN2COR_CONFIG["SEN2COR_AUX_DIR"]}:/mnt/aux_data',
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

    versions_supported = ['2.10.0', '2.8.0', '2.5.5']

    err = None
    for version in versions_supported:
        out, err = _safe_execute(scene_id, input_dir, output_dir, docker_container_work_dir, version=version, **env)
        if out:
            return out

    raise RuntimeError(f'Could not execute Sen2Cor using {versions_supported} - {err}')
