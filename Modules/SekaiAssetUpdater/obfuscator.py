import aiofiles
from pathlib import Path
from typing import Union


async def deobfuscate(data: bytes) -> bytes:
    """Deobfuscate a bytes object."""
    if data[:4] == b"\x20\x00\x00\x00":
        data = data[4:]
    elif data[:4] == b"\x10\x00\x00\x00":
        data = data[4:]
        header = bytes(a ^ b for a, b in zip(data[:128], (b"\xff" * 5 + b"\x00" * 3) * 16))
        data = header + data[128:]
    return data


async def deobfuscate_save_file(data: bytes, save_path: Union[Path, str]) -> None:
    """Deobfuscate and save a bytes object to a file."""
    _data = await deobfuscate(data)
    save_path = Path(save_path)
    save_path.parent.mkdir(parents=True, exist_ok=True)
    async with aiofiles.open(save_path, "wb") as f:
        await f.write(_data)


async def deobfuscate_file(asset: Union[Path, str]) -> None:
    """Deobfuscate an obfuscated file."""
    asset = Path(asset)
    async with aiofiles.open(asset, "br+") as f:
        data = await f.read()
        data = await deobfuscate(data)
        await f.seek(0)
        await f.write(data)
        await f.truncate()


async def obfuscate_file(asset: Union[Path, str]):
    """Obfuscate a deobfuscated file."""
    asset = Path(asset)
    async with aiofiles.open(asset, "br+") as f:
        data = await f.read()
        header = bytes(a ^ b for a, b in zip(data[:128], (b"\xff" * 5 + b"\x00" * 3) * 16))
        data = b"\x10\x00\x00\x00" + header + data[128:]
        await f.seek(0)
        await f.write(data)
        await f.truncate()
