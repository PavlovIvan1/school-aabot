import asyncio
import os

# Отдельный воркер только для контура с метриками.
os.environ["CHECK_INFO_WORKER"] = "1"
os.environ["ENABLE_METRICS_SYNC"] = "1"
os.environ["METRICS_ONLY"] = "1"

from bot import check_info  # noqa: E402


async def main() -> None:
    print("[METRICS_WORKER] started")
    await check_info()


if __name__ == "__main__":
    asyncio.run(main())
