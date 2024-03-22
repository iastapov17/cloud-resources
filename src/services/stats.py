import logging
import os
import pickle

import numpy as np
import typing as tp

from collections import OrderedDict

from src import models
from src import utils

from src.clients.stats import StatsClient
from src.settings import settings


logger = logging.getLogger(__name__)


def _cal_percent(base, perc):
    return (base * perc) / 100


class StatsService:
    _stats_client: StatsClient
    memory: OrderedDict[models.Stat] = {}

    vm_cpu_overhead: float = 0.05
    vm_ram_overhead: float = 0.3

    vm_cpu_request: float = 0.001
    vm_ram_request: float = 0.005

    db_cpu_overhead: float = 0.05
    db_ram_overhead: float = 0.512

    db_cpu_request: float = 0.001
    db_ram_request: float = 0.03

    is_overhead_calc: bool = False

    PATH = "memory.pickle"

    def __init__(self, stats_client: StatsClient) -> None:
        self._stats_client: StatsClient = stats_client

    async def update_stats(self, prices) -> None:
        stat = await self._stats_client.get()
        if not stat:
            return None

        if len(self.memory) > settings.memory_size:
            self.memory.pop(next(iter(self.memory)))

        self.memory[stat.timestamp] = stat
        self._calculate_overhead(prices)
        logger.info(f"Memory size: {len(self.memory)}")
        if not settings.prod:
            self._save_memory()

    def get_last_stat(self) -> tp.Optional[models.Stat]:
        if not self.memory:
            return None
        return self.memory[next(iter(reversed(self.memory)))]

    def _calculate_overhead(self, prices):
        if len(self.memory) < 2:
            return None

        res_iter = iter(reversed(self.memory))

        first_item = self.memory[next(res_iter)]
        second_item = self.memory[next(res_iter)]

        if (
            first_item.vm_cpu_load == 0
            or first_item.vm_ram_load == 0
            or first_item.db_cpu_load == 0
            or first_item.db_ram_load == 0
            or second_item.vm_cpu_load == 0
            or second_item.vm_ram_load == 0
            or second_item.db_cpu_load == 0
            or second_item.db_ram_load == 0
        ):
            self.is_overhead_calc = False
            return None

        f_vm_cnt = len(
            utils.choose_resource(
                prices[models.ResourceType.VM], first_item.vm_cpu, first_item.vm_ram
            )
        )
        f_db_cnt = len(
            utils.choose_resource(
                prices[models.ResourceType.DB], first_item.db_cpu, first_item.db_ram
            )
        )

        s_vm_cnt = len(
            utils.choose_resource(
                prices[models.ResourceType.VM], second_item.vm_cpu, second_item.vm_ram
            )
        )
        s_db_cnt = len(
            utils.choose_resource(
                prices[models.ResourceType.DB], second_item.db_cpu, second_item.db_ram
            )
        )

        result = []
        data = (
            (f_vm_cnt, s_vm_cnt, "vm_cpu", "vm_cpu_load",),
            (f_vm_cnt, s_vm_cnt, "vm_ram", "vm_ram_load",),
            (f_db_cnt, s_db_cnt, "db_cpu", "db_cpu_load",),
            (f_db_cnt, s_db_cnt, "db_ram", "db_ram_load",),
        )
        for f_cnt, s_cnt, res, load in data:
            m = np.array([[f_cnt, first_item.requests], [s_cnt, second_item.requests]])
            v = np.array(
                [
                    _cal_percent(getattr(first_item, res), getattr(first_item, load)),
                    _cal_percent(getattr(second_item, res), getattr(second_item, load)),
                ]
            )
            over, request = np.linalg.solve(m, v)
            result.append([over.item(), request.item()])

        if any(x[0] < 0 or x[1] < 0 for x in result):
            return None

        self.vm_cpu_overhead, self.vm_cpu_request = result[0]
        self.vm_ram_overhead, self.vm_ram_request = result[1]
        self.db_cpu_overhead, self.db_cpu_request = result[2]
        self.db_ram_overhead, self.db_ram_request = result[3]
        self.is_overhead_calc = True

    def get_overhead(self, resource_type):
        if resource_type == models.ResourceType.VM:
            return self._get_vm_overhead()
        return self._get_db_overhead()

    def _get_vm_overhead(self):
        return self.vm_cpu_overhead, self.vm_ram_overhead

    def _get_db_overhead(self):
        return self.db_cpu_overhead, self.db_ram_overhead

    def get_need_resource(self, prices, resource_type, requests: int):
        if resource_type == models.ResourceType.VM:
            return self._get_vm_resource(prices, requests)
        return self._get_db_resource(prices, requests)

    def _get_vm_resource(self, prices, requests: int):
        return utils.choose_optimal_resources(
            prices,
            requests,
            self.vm_cpu_request,
            self.vm_ram_request,
            self.vm_cpu_overhead,
            self.vm_ram_overhead,
        )

    def _get_db_resource(self, prices, requests: int):
        return utils.choose_optimal_resources(
            prices,
            requests,
            self.db_cpu_request,
            self.db_ram_request,
            self.db_cpu_overhead,
            self.db_ram_overhead,
        )

    def _save_memory(self):
        with open(self.PATH, "wb") as f:
            pickle.dump(self.memory, f)

    def load_memory(self):
        if not os.path.exists(self.PATH):
            return None
        with open(self.PATH, "rb") as f:
            self.memory = pickle.load(f)
