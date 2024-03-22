import asyncio
import typing as tp

from src import models

from src.clients.price import PriceClient
from src.clients.resource import ResourceClient
from src.settings import settings


class ResourceService:
    _price_client: PriceClient
    _resource_client: ResourceClient

    def __init__(
        self, price_client: PriceClient, resource_client: ResourceClient
    ) -> None:
        self._price_client: PriceClient = price_client
        self._resource_client: ResourceClient = resource_client

    async def init(self, prices) -> None:
        tasks = []

        min_vm = min(prices[models.ResourceType.VM], key=lambda x: x.cost)
        max_vm = max(prices[models.ResourceType.VM], key=lambda x: x.cost)
        cnt = round(max_vm.cost / min_vm.cost) - 1
        for _ in range(max(1, cnt)):
            tasks.append(self.add(models.ResourceType.VM, min_vm))

        min_db = min(prices[models.ResourceType.DB], key=lambda x: x.cost)
        max_db = max(prices[models.ResourceType.DB], key=lambda x: x.cost)
        cnt = round(max_db.cost / min_db.cost) - 1
        for _ in range(max(1, cnt)):
            tasks.append(self.add(models.ResourceType.DB, min_db))

        await asyncio.gather(*(tasks if settings.prod else []))

    async def get(self) -> tp.List[models.GetResource]:
        return await self._resource_client.get()

    async def add(
        self, resource_type: models.ResourceType, price: models.Price
    ) -> None:
        await self._resource_client.post(
            models.PostResource(cpu=price.cpu, ram=price.ram, type=resource_type,)
        )

    async def put(self, item_id: int, pod: models.PostResource) -> None:
        await self._resource_client.put(item_id, pod)

    async def delete_by_id(self, item_id: int) -> None:
        await self._resource_client.delete(item_id)

    async def delete_resources(self) -> None:
        resources = await self._resource_client.get()

        tasks = [self._resource_client.delete(resource.id) for resource in resources]
        await asyncio.gather(*(tasks if settings.prod else []))
