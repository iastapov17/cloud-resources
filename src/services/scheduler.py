import asyncio
import datetime
import logging
import typing as tp
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

from src import models
from src import utils

from src.clients.price import PriceClient
from src.services.resource import ResourceService
from src.services.stats import StatsService
from src.services.predict import PredictService
from src.settings import settings

logger = logging.getLogger(__name__)


class SchedulerService:
    dates = []
    vm_cpu_load = []
    vm_ram_load = []
    db_cpu_load = []
    db_ram_load = []

    def __init__(
        self,
        price_client: PriceClient,
        resource_service: ResourceService,
        stat_service: StatsService,
        predict_service: PredictService,
    ):
        self._price_client: PriceClient = price_client
        self._resource_service: ResourceService = resource_service
        self._stat_service: StatsService = stat_service
        self._predict_service: PredictService = predict_service

    async def task(self):
        logger.info("#task: start")
        await self.calculate()

    async def calculate(self):
        prices = await self._price_client.get_grouped_prices()

        await self._stat_service.update_stats(prices)
        self._predict_service.predict()

        current_resources = await self._resource_service.get()
        if not current_resources:
            await self._resource_service.init(prices)
        else:
            await self.update(current_resources, prices)

        self._clear_data()
        self._plot()

    async def update(self, current_resources, prices):
        resources = {}
        for resource in current_resources:
            resources.setdefault(resource.type, []).append(resource)

        self.dates.append(datetime.datetime.now())
        await self.update_by_type(
            models.ResourceType.VM, resources, prices,
        )
        await self.update_by_type(
            models.ResourceType.DB, resources, prices,
        )

    async def update_by_type(
        self,
        resource_type: models.ResourceType,
        resources: tp.Dict[models.ResourceType, tp.List[models.GetResource]],
        all_prices: tp.Dict[models.ResourceType, tp.List[models.Price]],
    ):
        pods = resources.get(resource_type, [])
        prices = all_prices[resource_type]
        active_pods = list(pod for pod in pods if not pod.failed)

        pod_count = len(pods)
        active_pod_count = len(active_pods)
        not_active_pods_count = pod_count - active_pod_count

        cpu_load, ram_load = self._get_load(active_pods)
        cpu = sum(pod.cpu for pod in active_pods)
        ram = sum(pod.ram for pod in active_pods)
        is_app_offline = self._is_offline(active_pods)

        cpu_overhead, ram_overhead = self._stat_service.get_overhead(resource_type)
        abs_cpu_load, abs_ram_load = self._get_abs_load(
            active_pods, cpu, ram, cpu_overhead, ram_overhead
        )

        if resource_type == resource_type.VM:
            self.vm_cpu_load.append(abs_cpu_load)
            self.vm_ram_load.append(abs_ram_load)
        elif resource_type == resource_type.DB:
            self.db_cpu_load.append(abs_cpu_load)
            self.db_ram_load.append(abs_ram_load)

        logger.info(
            "#updateByType: type = [%s], pod count = [%s], active pod count = [%s], "
            "not active pod count = [%s], cpu load = [%s], ram load = [%s]",
            resource_type,
            pod_count,
            active_pod_count,
            not_active_pods_count,
            cpu_load,
            ram_load,
        )

        cpu_diff, ram_diff = self.relative_average_diff(
            resource_type, abs_cpu_load, abs_ram_load
        )
        if ram_diff >= settings.delta or cpu_diff >= settings.delta:
            return None

        predicted = False
        need_pods = []
        p_need_cpu, p_need_ram = 0, 0
        if (
            self._stat_service.is_overhead_calc
            and self._predict_service.is_request_predicted
        ):
            predicted = True
            for request_cnt in self._predict_service.requests:
                p_need_pods = self._stat_service.get_need_resource(
                    prices, resource_type, request_cnt,
                )

                pred_need_cpu = sum(pod.cpu for pod in p_need_pods)
                pred_need_ram = sum(pod.ram for pod in p_need_pods)

                p_abs_cpu = pred_need_cpu - len(p_need_pods) * cpu_overhead
                p_abs_ram = pred_need_ram - len(p_need_pods) * ram_overhead

                if (
                    pred_need_cpu <= 0
                    or pred_need_ram <= 0
                    or p_abs_cpu * settings.pod_load_max_percent < abs_cpu_load
                    or p_abs_ram * settings.pod_load_max_percent < abs_ram_load
                ):
                    continue
                p_need_cpu = p_abs_cpu
                p_need_ram = p_abs_ram
                need_pods = p_need_pods

        if not predicted and len(self._stat_service.memory) >= settings.min_memory_size:
            return None

        need_cpu, need_ram = p_need_cpu, p_need_ram
        if need_cpu <= 0 or need_ram <= 0:
            need_cpu = abs_cpu_load / settings.pod_load_max_percent
            need_ram = abs_ram_load / settings.pod_load_max_percent
            need_pods = utils.choose_resource(
                prices, need_cpu, need_ram, cpu_overhead, ram_overhead
            )

        if not need_pods:
            return None

        pods = sorted(pods, key=lambda x: (x.cpu, x.ram), reverse=True)
        need_pods = sorted(need_pods, key=lambda x: (x.cpu, x.ram), reverse=True)

        if is_app_offline:
            to_create, to_update, to_delete = self._calculate_vm_changes_offline(
                pods, need_pods,
            )
        else:
            to_create, to_update, to_delete = self._calculate_vm_changes(
                pods, need_pods, need_cpu, need_ram, cpu_overhead, ram_overhead,
            )

        tasks = []
        for resource in to_create:
            tasks.append(self._resource_service.add(resource_type, resource))
        for item_id, resource in to_update:
            tasks.append(self._resource_service.put(item_id, resource))
        for item_id in to_delete:
            tasks.append(self._resource_service.delete_by_id(item_id))

        await asyncio.gather(*(tasks if settings.prod else []))

    def relative_average_diff(
        self, resource_type: models.ResourceType, cpu_value, ram_value
    ):
        if resource_type == models.ResourceType.VM:
            return self._vm_relative_average_diff(cpu_value, ram_value)
        return self._db_relative_average_diff(cpu_value, ram_value)

    def _vm_relative_average_diff(self, cpu_value, ram_value):
        if len(self.vm_ram_load) < 4 and len(self.vm_cpu_load) < 4:
            return 5, 5

        avg_ram = max(
            sum(self.vm_ram_load[-(settings.gap + 1) : -1]) / settings.gap, 0.1
        )
        avg_cpu = max(
            sum(self.vm_cpu_load[-(settings.gap + 1) : -1]) / settings.gap, 0.1
        )

        return (
            abs((avg_cpu - cpu_value) / avg_cpu),
            abs((avg_ram - ram_value) / avg_ram),
        )

    def _db_relative_average_diff(self, cpu_value, ram_value):
        if len(self.db_ram_load) < 4 and len(self.db_cpu_load) < 4:
            return 5, 5

        avg_ram = max(
            sum(self.db_ram_load[-(settings.gap + 1) : -1]) / settings.gap, 0.1
        )
        avg_cpu = max(
            sum(self.db_cpu_load[-(settings.gap + 1) : -1]) / settings.gap, 0.1
        )

        return (
            abs((avg_cpu - cpu_value) / avg_cpu),
            abs((avg_ram - ram_value) / avg_ram),
        )

    @staticmethod
    def _calculate_vm_changes_offline(
        pods: tp.List[models.GetResource], need_pods: tp.List[models.Price],
    ):
        to_create, to_update = [], []
        while pods and need_pods:
            curr_pod = pods.pop(0)
            max_pod = need_pods.pop(0)

            if max_pod.cpu == curr_pod.cpu and max_pod.ram == curr_pod.ram:
                continue
            elif max_pod.cpu >= curr_pod.cpu or max_pod.ram >= curr_pod.ram:
                to_update.append(
                    (
                        curr_pod.id,
                        models.PostResource(
                            cpu=max_pod.cpu, ram=max_pod.ram, type=max_pod.type,
                        ),
                    )
                )
            else:
                to_create.append(max_pod)

        to_create.extend(need_pods)
        to_delete = [pod.id for pod in pods]
        return to_create, to_update, to_delete

    @staticmethod
    def _calculate_vm_changes(
        pods: tp.List[models.GetResource],
        need_pods: tp.List[models.Price],
        need_cpu: int,
        need_ram: int,
        cpu_overhead: float = 0,
        ram_overhead: float = 0,
    ):
        active_pods = [pod for pod in pods if not pod.failed]
        leave_pods = utils.choose_resource_exists(
            active_pods, need_cpu, need_ram, cpu_overhead, ram_overhead
        )
        dict_pods = {pod.id: pod for pod in pods}
        if not leave_pods:
            for pod in pods:
                ind = None
                for nid, n_pod in enumerate(need_pods):
                    if n_pod.cpu == pod.cpu and n_pod.ram == pod.ram:
                        ind = nid
                        break
                if ind is not None:
                    need_pods.pop(ind)

            return need_pods, [], []

        for pod_id in set(leave_pods):
            pod = dict_pods.pop(pod_id, None)
            if pod is None:
                continue

            ind = None
            for nid, n_pod in enumerate(need_pods):
                if n_pod.cpu == pod.cpu and n_pod.ram == pod.ram:
                    ind = nid
                    break
            if ind is not None:
                need_pods.pop(ind)

        pods = sorted(dict_pods.values(), key=lambda x: (x.cpu, x.ram), reverse=True)

        to_create, to_update = [], []
        while pods and need_pods:
            curr_pod = pods.pop(0)
            max_pod = need_pods.pop(0)

            if max_pod.cpu == curr_pod.cpu and max_pod.ram == curr_pod.ram:
                continue
            else:
                to_update.append(
                    (
                        curr_pod.id,
                        models.PostResource(
                            cpu=max_pod.cpu, ram=max_pod.ram, type=max_pod.type,
                        ),
                    )
                )

        to_create.extend(need_pods)
        to_delete = [pod.id for pod in pods]
        return to_create, to_update, to_delete

    def _is_offline(self, pods: tp.List[models.GetResource]):
        cpu_load, ram_load = self._get_load(pods)
        if cpu_load >= settings.max_load or ram_load >= settings.max_load:
            return True
        return False

    @staticmethod
    def _get_load(pods: tp.List[models.GetResource]):
        cpu_load = sum(pod.cpu_load * pod.cpu for pod in pods)
        ram_load = sum(pod.ram_load * pod.ram for pod in pods)
        return cpu_load, ram_load

    def _get_abs_load(
        self, pods: tp.List[models.GetResource], cpu, ram, cpu_overhead, ram_overhead
    ):
        cpu_load, ram_load = self._get_load(pods)
        return (
            cpu * cpu_load / 100 - len(pods) * cpu_overhead,
            ram * ram_load / 100 - len(pods) * ram_overhead,
        )

    def _plot(self):
        fig, axs = plt.subplots(3, gridspec_kw={"wspace": 0.5, "hspace": 0.5})
        fig.suptitle("LOADS")

        dates = self._stat_service.memory.keys()
        vm_cpu_load = [
            item.vm_cpu_load for _, item in self._stat_service.memory.items()
        ]
        vm_ram_load = [
            item.vm_ram_load for _, item in self._stat_service.memory.items()
        ]
        db_cpu_load = [
            item.db_cpu_load for _, item in self._stat_service.memory.items()
        ]
        db_ram_load = [
            item.db_ram_load for _, item in self._stat_service.memory.items()
        ]

        axs[0].title.set_text("VM")
        axs[0].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        axs[0].axhline(y=settings.max_load, color="red", linestyle="--")
        axs[0].axhline(y=settings.pod_load_max, color="orange", linestyle="--")

        axs[0].scatter(dates, vm_cpu_load, color="green", s=10)
        axs[0].scatter(dates, vm_ram_load, color="blue", s=10)

        axs[0].plot(dates, vm_cpu_load, color="green", label="CPU")
        axs[0].plot(dates, vm_ram_load, color="blue", label="RAM")
        axs[0].legend()

        axs[1].title.set_text("DB")
        axs[1].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        axs[1].axhline(y=settings.max_load, color="red", linestyle="--")
        axs[1].axhline(y=settings.pod_load_max, color="orange", linestyle="--")

        axs[1].scatter(dates, db_cpu_load, color="green", s=10)
        axs[1].scatter(dates, db_ram_load, color="blue", s=10)

        axs[1].plot(dates, db_cpu_load, color="green", label="CPU")
        axs[1].plot(dates, db_ram_load, color="blue", label="RAM")
        axs[1].legend()

        data = [item.requests for _, item in self._stat_service.memory.items()]
        axs[2].title.set_text("REQUESTS")
        axs[2].xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))

        axs[2].scatter(self._stat_service.memory.keys(), data, color="orange", s=10)
        axs[2].plot(
            self._stat_service.memory.keys(), data, color="red", label="REQUESTS"
        )

        axs[2].legend()

        plt.draw()
        plt.show()

    def _clear_data(self):
        self.dates = self.dates[-settings.max_data_size :]
        self.db_cpu_load = self.db_cpu_load[-settings.max_data_size :]
        self.db_ram_load = self.db_ram_load[-settings.max_data_size :]
        self.db_cpu_load = self.db_cpu_load[-settings.max_data_size :]
        self.db_ram_load = self.db_ram_load[-settings.max_data_size :]
