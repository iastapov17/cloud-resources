import typing as tp

from functools import wraps

from pydantic import TypeAdapter

from pulp import (
    PULP_CBC_CMD,
    LpProblem,
    LpBinary,
    LpMinimize,
    LpVariable,
    LpInteger,
    lpSum,
    LpStatus,
    value,
)

from src import models
from src.settings import settings


def map_result(function: tp.Callable) -> tp.Callable:
    @wraps(function)
    async def wrapper(*args, **kwargs):
        return_type = tp.get_type_hints(wrapper).get("return")
        result = await function(*args, **kwargs)
        return (
            TypeAdapter(return_type).validate_python(result)
            if return_type and result
            else result
        )

    return wrapper


def choose_resource(
    data: tp.List[models.Price],
    need_cpu: int,
    need_ram: int,
    cpu_overhead: float = 0,
    ram_overhead: float = 0,
):
    resource_types_cnt = len(data)
    prob = LpProblem("Minimize_Cost", LpMinimize)

    x = [LpVariable(f"x{i}", 0, None, LpInteger) for i in range(resource_types_cnt)]

    prob += lpSum(
        x[i] * (data[i].cost - settings.penalty) for i in range(resource_types_cnt)
    )

    prob += (
        lpSum(x[i] * (data[i].cpu - cpu_overhead) for i in range(resource_types_cnt))
        >= need_cpu
    )
    prob += (
        lpSum(x[i] * (data[i].ram - ram_overhead) for i in range(resource_types_cnt))
        >= need_ram
    )
    prob.solve(PULP_CBC_CMD(msg=False))

    return [data[i] for i in range(resource_types_cnt) for _ in range(int(value(x[i])))]


def choose_resource_exists(
    pods: tp.List[models.GetResource],
    need_cpu: int,
    need_ram: int,
    cpu_overhead: float = 0,
    ram_overhead: float = 0,
):
    pods_cnt = len(pods)
    prob = LpProblem("Minimize_Active_VMs", LpMinimize)

    x = [LpVariable(f"x{i}", 0, 1, LpBinary) for i in range(pods_cnt)]

    prob += lpSum(x)

    prob += lpSum(x[i] * (pods[i].cost - settings.penalty) for i in range(pods_cnt))
    prob += (
        lpSum(x[i] * (pods[i].cpu - cpu_overhead) for i in range(pods_cnt)) >= need_cpu
    )
    prob += (
        lpSum(x[i] * (pods[i].ram - ram_overhead) for i in range(pods_cnt)) >= need_ram
    )

    prob.solve(PULP_CBC_CMD(msg=False))
    if LpStatus[prob.status] == "Optimal":
        return [pods[i].id for i in range(pods_cnt) if value(x[i]) == 1]
    else:
        return []


def choose_optimal_resources(
    data: tp.List[models.Price],
    requests: int,
    request_cpu: float,
    request_ram: float,
    overhead_cpu: float,
    overhead_ram: float,
):
    resource_types_cnt = len(data)
    model = LpProblem("Minimize_Cost", LpMinimize)
    vm_vars = LpVariable.dicts(
        "VM", range(resource_types_cnt), lowBound=0, cat="Integer"
    )

    model += lpSum(
        vm_vars[i] * (data[i].cost - settings.penalty)
        for i in range(resource_types_cnt)
    )
    model += (
        lpSum(
            vm_vars[i] * (settings.pod_load_max_percent * data[i].cpu - overhead_cpu)
            for i in range(resource_types_cnt)
        )
        >= requests * request_cpu
    )
    model += (
        lpSum(
            vm_vars[i] * (settings.pod_load_max_percent * data[i].ram - overhead_ram)
            for i in range(resource_types_cnt)
        )
        >= requests * request_ram
    )

    model.solve(PULP_CBC_CMD(msg=False))
    optimal = []

    for i in range(resource_types_cnt):
        num = int(value(vm_vars[i]))
        if num > 0:
            optimal.extend([data[i]] * num)
    return optimal
