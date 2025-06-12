import gc
import orjson
import UnityPy
import logging
import traceback
import coloredlogs
from pathlib import Path
from typing import Union, Optional

from ..log_format import LOG_FORMAT, FIELD_STYLE
from .acb_extractor import extract_acb

logger = logging.getLogger(__name__)
coloredlogs.install(level="DEBUG", logger=logger, fmt=LOG_FORMAT, field_styles=FIELD_STYLE)


def unpack_asset(
    asset_path: Union[Path, str],
    save_dir: Union[Path, str],
    fallback_unity_version: Optional[str] = "2020.3.32f1",
    binary_data: Optional[bytes] = None,
):
    """Unpack Unity asset as needed."""
    save_dir = Path(save_dir)
    UnityPy.config.FALLBACK_UNITY_VERSION = fallback_unity_version

    try:
        if binary_data:
            _unity_file = UnityPy.load(binary_data)
        else:
            _unity_file = UnityPy.load(asset_path.as_posix())
        logger.info(f"UnityPy loaded: {asset_path}")
    except Exception as e:
        traceback.print_exc()
        raise

    for unityfs_path, unityfs_obj in _unity_file.container.items():
        try:
            _relpath = Path(unityfs_path).relative_to("assets/sekai/assetbundle/resources")
        except ValueError:
            logger.warning(f"Non-relative path detected: {unityfs_path}")
            _relpath = Path(unityfs_path)
        _save_path = save_dir / _relpath
        _save_dir = _save_path.parent
        _save_dir.mkdir(parents=True, exist_ok=True)
        try:
            match unityfs_obj.type.name:
                case "Texture2D" | "Sprite":
                    data = unityfs_obj.read()
                    data.image.save(_save_path.with_suffix(".png"))
                case "TextAsset":
                    if _relpath.name.endswith(".acb.bytes"):
                        logger.debug(f"UnityPy handling ACB file: {unityfs_path}")
                        data = unityfs_obj.read()
                        extract_acb(_save_path, binary_data=data.m_Script.encode("utf-8", "surrogateescape"))
                    else:
                        data = unityfs_obj.read()
                        with open(_save_path, "wb") as f:
                            f.write(data.m_Script.encode("utf-8", "surrogateescape"))
                case "MonoBehaviour":
                    logger.debug(f"UnityPy saving json: {unityfs_path}")
                    tree = None
                    try:
                        if unityfs_obj.serialized_type.node:
                            tree = unityfs_obj.read_typetree()
                    except AttributeError:
                        tree = unityfs_obj.read_typetree()
                    logger.debug("Saving MonoBehaviour %s to %s", unityfs_path, _save_path)
                    with open(_save_path, "wb") as file:
                        file.write(orjson.dumps(tree, option=orjson.OPT_INDENT_2))
        except Exception as e:
            traceback.print_exc()
            continue

    del _unity_file
    gc.collect()
