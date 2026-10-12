import asyncio

from .app import TelefetchApp
from .config import Settings


def main() -> None:
    async def run() -> None:
        # Pyrogram captures the current event loop while Client is constructed.
        # Build the app after asyncio.run() has installed its runtime loop so
        # media-download executors and worker tasks share that same loop.
        await TelefetchApp(Settings.from_env()).run()

    asyncio.run(run())


if __name__ == "__main__":
    main()
