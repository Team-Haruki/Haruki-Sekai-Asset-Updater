from typing import Optional
from enum import Enum, IntEnum
from pydantic import BaseModel


class SekaiServerInfo(BaseModel):
    server: str
    asset_info_url: str
    nuverse_asset_version_url: Optional[str] = None
    nuverse_fallback_app_version: Optional[str] = None
    asset_url: str
    require_cookies: Optional[bool] = False
    headers: Optional[dict] = None
    enabled: Optional[bool] = True
    aes_key: Optional[bytes] = None
    aes_iv: Optional[bytes] = None
    unity_version: Optional[str] = '2020.3.32f1'


class SekaiServerRegion(Enum):
    JP = 'jp'
    EN = 'en'
    TW = 'tw'
    KR = 'kr'
    CN = 'cn'


class SekaiApiHttpStatus(IntEnum):
    OK = 200
    CLIENT_ERROR = 400
    SESSION_ERROR = 403
    NOT_FOUND = 404
