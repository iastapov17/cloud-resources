import asyncio
import logging
import sys
import warnings

from src.clients.resource import ResourceClient
from src.clients.price import PriceClient
from src.services.predict import PredictService

from src.services.resource import ResourceService
from src.services.scheduler import SchedulerService
from src.services.stats import StatsService
from src.settings import settings
from src.injection import configure, on


warnings.filterwarnings("ignore")

logging.basicConfig(
    stream=sys.stderr,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
)
logger = logging.getLogger(__name__)


async def main() -> None:
    await configure()

    price_client = PriceClient()
    resource_client = ResourceClient()

    resource_service = ResourceService(
        price_client=price_client, resource_client=resource_client,
    )
    stat_service = on(StatsService)

    scheduler_service = SchedulerService(
        resource_service=resource_service,
        price_client=price_client,
        stat_service=stat_service,
        predict_service=PredictService(stat_service),
    )
    if not settings.prod:
        stat_service.load_memory()

    while True:
        try:
            await scheduler_service.task()
        except Exception as exc:
            logging.error(f"Task failed: {exc}")
        await asyncio.sleep(settings.sleep_second)


if __name__ == "__main__":
    asyncio.run(main())
