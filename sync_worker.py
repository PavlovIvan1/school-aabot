import asyncio
import os

# Воркерный режим: включаем метрики и тяжелый sync-цикл.
os.environ["CHECK_INFO_WORKER"] = "1"
os.environ["ENABLE_METRICS_SYNC"] = "1"

from bot import check_info  # noqa: E402


async def main() -> None:
    print("[SYNC_WORKER] started")
    await check_info()


if __name__ == "__main__":
    asyncio.run(main())
