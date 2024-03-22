import httpx
import logging
import typing as tp

from urllib.parse import urljoin

from src import models
from src.utils import map_result
from src.settings import settings

logger = logging.getLogger(__name__)


class StatsClient:
    URL: str = "/api/statistic"

    @map_result
    async def get(self) -> tp.Optional[models.Stat]:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url=urljoin(settings.host, self.URL), params=self._params,
            )
            if response.is_success:
                logger.info(f"Success get stats. Body: {response.json()}")
                return response.json()

            logger.error(
                f"Failed get stats. Status: {response.status_code} Body: {response.text}"
            )
            return None

    @property
    def _params(self):
        return {"token": settings.token}
