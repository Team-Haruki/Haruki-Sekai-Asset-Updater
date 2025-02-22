import pygit2
from pathlib import Path
from Modules.SekaiAssetUpdater.model import SekaiServerRegion, SekaiServerInfo

HOST = '0.0.0.0'
PORT = 22344
WORK_DIR = Path(__file__).parent # Configure it if you need
PROXIES = ['http://127.0.0.1:7890']  # Configure proxies here

ASSET_SAVE_DIRS = {
    SekaiServerRegion.JP: WORK_DIR,
    SekaiServerRegion.TW: WORK_DIR,
    SekaiServerRegion.KR: WORK_DIR,
    SekaiServerRegion.EN: WORK_DIR,
    SekaiServerRegion.CN: WORK_DIR
}

# Sekai server configuration
SEKAI_SERVERS = {
    SekaiServerRegion.JP: SekaiServerInfo(
        server=SekaiServerRegion.JP.value,
        asset_info_url='',
        asset_url='',
        require_cookies=True,
        headers={},
        aes_key=b'',
        aes_iv=b''
    ),
    SekaiServerRegion.EN: SekaiServerInfo(
        server=SekaiServerRegion.EN.value,
        asset_info_url='',
        asset_url='',
        headers={},
        aes_key=b'',
        aes_iv=b''
    ),
    SekaiServerRegion.TW: SekaiServerInfo(
        server=SekaiServerRegion.TW.value,
        nuverse_asset_version_url='',
        asset_info_url='',
        asset_url='',
        headers={},
        aes_key=b'',
        aes_iv=b''
    ),
    SekaiServerRegion.KR: SekaiServerInfo(
        server=SekaiServerRegion.KR.value,
        nuverse_asset_version_url='',
        asset_info_url='',
        asset_url='',
        headers={},
        aes_key=b'',
        aes_iv=b''
    ),
    SekaiServerRegion.CN: SekaiServerInfo(
        server=SekaiServerRegion.CN.value,
        enabled=False,  # CN server is disabled by default because it has not been online yet
        nuverse_asset_version_url='',
        asset_info_url='',
        asset_url='',
        headers={},
        aes_key=b'',
        aes_iv=b''
    )
}

# Logger configuration
LOG_FORMAT = '[%(asctime)s][%(levelname)s][%(name)s] %(message)s'
FIELD_STYLE = {
    'asctime': {'color': 'green'},
    'levelname': {'color': 'blue', 'bold': True},
    'name': {'color': 'magenta'},
    'message': {'color': 144, 'bright': False}
}
