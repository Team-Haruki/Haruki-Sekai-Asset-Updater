import asyncio
from typing import Tuple, Dict
from quart import Quart, jsonify, request, Response

from Modules.SekaiAssetUpdater.model import SekaiServerRegion
from Modules.SekaiAssetUpdater.updater import SekaiAssetUpdater
from configs import (
    PROXIES,
    ASSET_SAVE_DIRS,
    SEKAI_SERVERS,
    SKIP_PREFIXES,
    STARTAPP_PREFIXES,
    ONDEMAND_PREFIXES,
    AUTHORIZATION,
)

app = Quart(__name__)
lock = asyncio.Lock()


async def run_updater(server: SekaiServerRegion, data: Dict) -> None:
    async with lock:
        updater = SekaiAssetUpdater(
            server_info=SEKAI_SERVERS.get(server),
            save_dir=ASSET_SAVE_DIRS.get(server),
            skip_download_prefix=SKIP_PREFIXES.get(server),
            startapp_prefix=STARTAPP_PREFIXES.get(server),
            ondemand_prefix=ONDEMAND_PREFIXES.get(server),
            asset_version=data.get("assetVersion"),
            asset_hash=data.get("assetHash"),
            proxies=PROXIES,
        )
        await updater.init()
        await updater.run()
        await updater.close()


@app.route("/update_asset", methods=["POST"])
async def update_asset() -> Tuple[Response, int]:
    if request.headers.get("Authorization") == f"Bearer {AUTHORIZATION}":
        data = await request.get_json()
        server = SekaiServerRegion(data["server"])
        data["server"] = server

        if not lock.locked():
            asyncio.create_task(run_updater(server, data))
            return jsonify({"message": "Asset updater start running"}), 200
        else:
            return jsonify({"message": "Asset updater is running"}), 409
    else:
        return jsonify({"message": "Invalid authorization header"}), 401
