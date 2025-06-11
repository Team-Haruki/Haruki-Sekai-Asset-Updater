from enum import Enum
from typing import Tuple, Optional


class SekaiServerJPEnvironment(Enum):
    """Fill this by yourself"""

    pass


def get_environment(profile: str) -> Optional[str]:
    _hash = {}  # Fill this by yourself
    try:
        _env = SekaiServerJPEnvironment(profile)
        return _hash.get(_env)
    except ValueError:
        return None
