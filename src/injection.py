from typing import Type, TypeVar

from injector import Injector, singleton

from src.clients.stats import StatsClient
from src.services.stats import StatsService


injector = Injector()
T = TypeVar("T")


def on(dependency_class: Type[T]) -> T:
    return injector.get(dependency_class)


async def configure():
    service = StatsService(StatsClient())
    injector.binder.bind(StatsService, to=service, scope=singleton)
