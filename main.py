import asyncio
from hypercorn.asyncio import serve
from hypercorn.config import Config

from app import app
from configs import HOST, PORT


async def run() -> None:
    config = Config()
    config.bind = [f'{HOST}:{PORT}']
    await serve(app, config)


if __name__ == '__main__':
    asyncio.run(run())
