import os
import asyncio
import logging
import traceback
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Tuple, Optional, Union
from urllib.parse import urlparse
from copy import deepcopy
import ujson as json
import aiofiles
import coloredlogs
from rich.progress import Progress, TaskID
from aiohttp import ClientSession, ClientProxyConnectionError
from tenacity import retry, stop_after_attempt, wait_fixed
from zoneinfo import ZoneInfo
from .jp_env import get_environment
from .obfuscator import deobfuscate
from .model import SekaiServerInfo, SekaiServerRegion, SekaiApiHttpStatus
from .cryptor import SekaiCryptor
from .unpacker import unpack_asset
from ..log_format import LOG_FORMAT, FIELD_STYLE

logger = logging.getLogger(__name__)
coloredlogs.install(level='DEBUG', logger=logger, fmt=LOG_FORMAT, field_styles=FIELD_STYLE)


class SekaiAssetUpdater:
    def __init__(self, server_info: SekaiServerInfo, save_dir: Union[Path, str], startapp_prefix: Tuple,
                 ondemand_prefix: Tuple, asset_version: str, asset_hash: Optional[str] = None,
                 proxies: Optional[Union[List, str]] = None) -> None:
        # Server configuration
        self.server = SekaiServerRegion(server_info.server)
        # Asset downloader configuration
        self.nuverse_fallback_app_version = server_info.nuverse_fallback_app_version
        self.nuverse_asset_version_url = server_info.nuverse_asset_version_url
        self.asset_info_url = server_info.asset_info_url
        self.require_cookies = server_info.require_cookies if server_info.require_cookies else False
        self.asset_url = server_info.asset_url
        self.headers = server_info.headers
        self.download_startapp_prefix = startapp_prefix
        self.download_ondemand_prefix = ondemand_prefix
        # Asset version configuration
        self.asset_version = asset_version
        self.asset_hash = asset_hash
        # Cryptor configuration
        self.cryptor = SekaiCryptor(server_info.aes_key, server_info.aes_iv)
        # Others configuration
        self.unity_version = server_info.unity_version
        self.save_dir = Path(save_dir)
        self.downloaded_assets_file = self.save_dir / 'downloaded_assets.json'
        self.proxies = proxies if proxies is not None else ['']
        self.progress = Progress()
        self.session: Optional[ClientSession] = None

    @staticmethod
    async def get_time_arg() -> str:
        _time = datetime.now(ZoneInfo('Asia/Tokyo'))
        time_format = _time.strftime('%Y%m%d%H%M%S')
        return f'?t={time_format}'

    @staticmethod
    async def _get_headers(headers: Dict, url: str) -> Dict:
        headers = deepcopy(headers)
        headers['Host'] = urlparse(url).hostname
        return headers

    # (Japanese server only) Parse CloudFront cookies
    async def _parse_cookies(self) -> None:
        if self.server == SekaiServerRegion.JP:
            headers = {
                "Accept": "*/*",
                "User-Agent": "ProductName/134 CFNetwork/1408.0.4 Darwin/22.5.0",
                "Connection": "keep-alive",
                "Accept-Language": "zh-CN,zh-Hans;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "X-Unity-Version": "2022.3.21f1"
            }
            async with ClientSession() as session:
                async with session.post(url="https://issue.sekai.colorfulpalette.org/api/signature",
                                        headers=headers) as response:
                    if response.status == 200:
                        self.headers['Cookie'] = response.headers.get("Set-Cookie")
        else:
            return None

    async def _load_downloaded_assets(self) -> Optional[Dict]:
        if not self.downloaded_assets_file.exists():
            self.downloaded_assets_file.parent.mkdir(parents=True, exist_ok=True)
            async with aiofiles.open(self.downloaded_assets_file, "w") as f:
                await f.write('{}')
            return {}
        async with aiofiles.open(self.downloaded_assets_file, "r") as f:
            return json.loads(await f.read())

    async def _save_downloaded_assets(self, data: Union[Dict, List]) -> None:
        async with aiofiles.open(self.downloaded_assets_file, "w") as f:
            await f.write(json.dumps(data))

    @retry(stop=stop_after_attempt(4), wait=wait_fixed(1))
    async def _request(self, url: str, headers: Dict, params: Optional[Dict] = None) -> Optional[bytes]:
        """Updater general request function"""
        for proxy in self.proxies:
            try:
                async with self.session.get(url=url, headers=headers, params=params, proxy=proxy) as response:
                    if response.status == SekaiApiHttpStatus.OK:
                        return await response.read()
                    else:
                        logger.warning(
                            f"{self.server.value.upper()} server updater request failed with status {response.status}")
                        return None
            except ClientProxyConnectionError:
                logger.warning(f"Failed to connect proxy {proxy}, switching proxy and retrying...")
                continue
            except Exception as e:
                traceback.print_exc()
                logger.error(f'{self.server.value.upper()} server updater request failed with error: {repr(e)}')
                return None

    # (Nuverse serve only) Get asset version url
    async def _get_asset_version(self, fallback_app_version: Optional[str] = None) -> Optional[str]:
        url = self.nuverse_asset_version_url.format(
            app_version=fallback_app_version if fallback_app_version else self.asset_version)
        headers = await self._get_headers(headers=self.headers, url=url)
        version = await self._request(url=url, headers=headers)
        if version is None:
            result = await self._get_asset_version(fallback_app_version=self.nuverse_fallback_app_version)
            if result is not None:
                return result
            else:
                logger.error(f'Failed to get {self.server.value.upper()}  server asset version')
        else:
            self.asset_version = fallback_app_version if fallback_app_version else self.asset_version
            return version.decode('utf-8')

    async def init(self) -> None:
        """Init for updater."""
        await self._parse_cookies()
        self.session = await ClientSession().__aenter__()
        if self.server == SekaiServerRegion.JP:
            profile = 'production'
            self.asset_info_url = self.asset_info_url.format(env=profile, hash=get_environment(profile),
                                                             asset_version=self.asset_version,
                                                             asset_hash=self.asset_hash)
            self.asset_url = self.asset_url.format(env=profile, hash=get_environment(profile),
                                                   asset_version=self.asset_version, asset_hash=self.asset_hash)

        elif self.server == SekaiServerRegion.EN:
            self.asset_info_url = self.asset_info_url.format(asset_version=self.asset_version)
            self.asset_url = self.asset_url.format(asset_version=self.asset_version, asset_hash=self.asset_hash)
        else:
            version = await self._get_asset_version(fallback_app_version=self.nuverse_fallback_app_version)
            self.asset_info_url = self.asset_info_url.format(app_version=self.asset_version, asset_version=version)
            self.asset_url = self.asset_url.format(app_version=self.asset_version)

    async def download_asset(self, semaphore: asyncio.Semaphore, asset_path: str, asset_hash: str,
                             progress: Optional[Progress] = None, task_id: Optional[TaskID] = None) -> Optional[
        Tuple[str, str]]:
        async with semaphore:
            url = self.asset_url + asset_path + await self.get_time_arg()
            headers = await self._get_headers(headers=self.headers, url=url)
            logger.info(f'{self.server.value.upper()} server updater downloading asset {asset_path}...')
            data = await self._request(url=url, headers=headers)
            if data:
                logger.info(f'{self.server.value.upper()} server updater downloaded asset {asset_path} successfully.')
                data = await deobfuscate(data)
                len(data)
                await asyncio.to_thread(unpack_asset, asset_path=asset_path, save_dir=self.save_dir, binary_data=data,
                                        fallback_unity_version=self.unity_version)
            else:
                logger.info(f'{self.server.value.upper()} server updater failed to download asset {asset_path}.')
                return None
            if progress:
                progress.update(task_id, advance=1)
            return asset_path, asset_hash

    async def get_assetbundle_info(self) -> Optional[Dict]:
        if self.server in [SekaiServerRegion.JP, SekaiServerRegion.EN]:
            url = self.asset_info_url + await self.get_time_arg()
        else:
            url = self.asset_info_url
        headers = await self._get_headers(headers=self.headers, url=url)
        logger.info(f'{self.server.value.upper()} server updater fetching asset bundle info...')
        data = await self._request(url=url, headers=headers)
        if data:
            logger.info(f'{self.server.value.upper()} server updater fetched asset bundle info successfully.')
            return await self.cryptor.unpack(data)
        else:
            logger.info(f'{self.server.value.upper()} server updater fetched asset bundle info failed.')

    async def run(self) -> None:
        _assetbundle_info = await self.get_assetbundle_info()
        _downloaded_assets = await self._load_downloaded_assets()
        _to_download_list = {}  # Pending download assets
        _to_download_bundle_mapping = {}
        _category_prefix_map = {
            'StartApp': self.download_startapp_prefix,
            'OnDemand': self.download_ondemand_prefix,
        }

        for _bundle, _bundle_info in _assetbundle_info['bundles'].items():
            _category = _bundle_info['category']
            if _category in _category_prefix_map and _bundle.startswith(_category_prefix_map[_category]):
                _downloaded_asset = _downloaded_assets.get(_bundle)
                if not _downloaded_asset or _bundle_info.get('hash') != _downloaded_asset:
                    if self.server in [SekaiServerRegion.JP, SekaiServerRegion.EN]:
                        _to_download_list[_bundle] = _bundle_info.get('hash')
                    else:
                        key = f'{_bundle_info.get("downloadPath")}/{_bundle}'
                        _to_download_list[key] = _bundle_info.get('hash')
                        _to_download_bundle_mapping[key] = _bundle

        _semaphore = asyncio.Semaphore(16)
        with Progress() as _progress:
            _task_id = _progress.add_task(f"Downloading {self.server.value.upper()} new assets...",
                                          total=len(_to_download_list))
            _download_tasks = [self.download_asset(_semaphore, bundle_path, bundle_hash, _progress, _task_id)
                               for bundle_path, bundle_hash in _to_download_list.items()]
            _result = await asyncio.gather(*_download_tasks)

        _result = [result for result in _result if result]
        if self.server in [SekaiServerRegion.TW, SekaiServerRegion.KR, SekaiServerRegion.CN]:
            _result = {_to_download_bundle_mapping.get(key): value for key, value in _result}
        else:
            _result = {key: value for key, value in _result}
        _downloaded_assets.update(_result)
        await self._save_downloaded_assets(_downloaded_assets)

    async def close(self) -> None:
        await self.session.close()
