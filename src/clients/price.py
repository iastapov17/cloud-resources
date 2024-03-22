import httpx
import logging
import typing as tp

from urllib.parse import urljoin

from src import models
from src.utils import map_result
from src.settings import settings

logger = logging.getLogger(__name__)


PriceType = tp.Dict[models.ResourceType, tp.List[models.Price]]


class PriceClient:
    URL: str = "/api/price"

    @map_result
    async def get(self) -> tp.List[models.Price]:
        async with httpx.AsyncClient() as client:
            response = await client.get(url=urljoin(settings.host, self.URL))
            if response.is_success:
                logger.info(f"Success get prices. Body: {response.json()}")
                return response.json()

            logger.error(
                f"Failed get prices. status: {response.status_code} body: {response.text}"
            )
            raise RuntimeError("Failed get prices")

    async def get_grouped_prices(self) -> PriceType:
        current_price = await self.get()
        result: PriceType = {}

        for item in current_price:
            result.setdefault(item.type, []).append(item)
        return result
