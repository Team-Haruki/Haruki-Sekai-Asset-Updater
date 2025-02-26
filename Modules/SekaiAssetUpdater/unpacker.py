import os
import gc
import UnityPy
import logging
import traceback
import coloredlogs
import ujson as json
from pathlib import Path
from typing import Union, Optional

from ..log_format import LOG_FORMAT, FIELD_STYLE
from .acb_extractor import extract_acb

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger, fmt=LOG_FORMAT, field_styles=FIELD_STYLE)


def unpack_asset(asset_path: Union[Path, str], save_dir: Union[Path, str],
                 fallback_unity_version: Optional[str] = '2020.3.32f1', binary_data: Optional[bytes] = None):
    """Unpack Unity asset as needed."""
    save_dir = str(save_dir)
    UnityPy.config.FALLBACK_UNITY_VERSION = fallback_unity_version

    try:
        if binary_data:
            _unity_file = UnityPy.load(binary_data)
        else:
            _unity_file = UnityPy.load(asset_path)
        logger.info(f'UnityPy loaded: {asset_path}')
    except Exception as e:
        traceback.print_exc()

    for unityfs_path, unityfs_obj in _unity_file.container.items():
        _relpath = os.path.relpath(unityfs_path, 'assets/sekai/assetbundle/resources/')
        _save_path = os.path.join(save_dir, _relpath)
        _save_dir = os.path.dirname(_save_path)
        os.makedirs(_save_dir, exist_ok=True)
        try:
            match unityfs_obj.type.name:
                case 'Texture2D' | 'Sprite':
                    data = unityfs_obj.read()
                    data.image.save(_save_path)
                case 'TextAsset':
                    if _relpath.endswith('.acb.bytes'):
                        logger.debug(f'UnityPy handling ACB file: {unityfs_path}')
                        data = unityfs_obj.read()
                        extract_acb(_save_path, binary_data=data.script)
                    else:
                        data = unityfs_obj.read()
                        with open(_save_path, 'wb') as f:
                            f.write(data.script)
                case 'MonoBehaviour':
                    logger.debug(f'UnityPy saving json: {unityfs_path}')
                    base_name = os.path.splitext(_save_path)[0]
                    if unityfs_obj.serialized_type.nodes:
                        tree = unityfs_obj.read_typetree()
                        fp = os.path.join(base_name + '.json')
                        os.makedirs(os.path.dirname(fp), exist_ok=True)
                        with open(fp, "wt", encoding="utf-8") as file:
                            file.write(json.dumps(tree, ensure_ascii=False, indent=4))
        except Exception as e:
            traceback.print_exc()
            continue

    del _unity_file
    gc.collect()
