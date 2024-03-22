import httpx
import logging
import typing as tp

from urllib.parse import urljoin

from src import models
from src.utils import map_result
from src.settings import settings

logger = logging.getLogger(__name__)


class ResourceClient:
    URL: str = "/api/resource"

    @map_result
    async def get(self) -> tp.List[models.GetResource]:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                url=urljoin(settings.host, self.URL), params=self._params,
            )
            if response.is_success:
                logger.info(f"Success get resources list. Body: {response.json()}")
                return response.json()

            logger.error(
                f"Failed get resources list. Status: {response.status_code} Body: {response.text}"
            )
            raise RuntimeError("Failed get resources list.")

    @map_result
    async def delete(self, item_id: int) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.delete(
                url=urljoin(settings.host, f"{self.URL}/{item_id}"),
                params=self._params,
            )
            if response.is_success:
                logger.info(f"Success delete resource by id: {item_id}")
                return None

            logger.error(
                f"Failed delete resource by id: {item_id}."
                f"Status: {response.status_code} Body: {response.text}"
            )
            raise RuntimeError("Failed delete resource")

    async def put(self, item_id: int, body: models.PostResource) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.put(
                url=urljoin(settings.host, f"{self.URL}/{item_id}"),
                params=self._params,
                json=body.model_dump(mode="json"),
            )
            if response.is_success:
                logger.info(f"Success put resource by id: {item_id}")
                return None

            logger.error(
                f"Failed put resource by id: {item_id}."
                f"Status: {response.status_code} Body: {response.text}"
            )
            raise RuntimeError("Failed delete resource")

    async def post(self, body: models.PostResource) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url=urljoin(settings.host, self.URL),
                params=self._params,
                json=body.model_dump(mode="json"),
            )
            if response.is_success:
                logger.info("Success create resource.")
                return None

            logger.error(
                f"Failed create resource. Status: {response.status_code} Body: {response.text}"
            )
            raise RuntimeError("Failed create resource")

    @property
    def _params(self):
        return {"token": settings.token}
