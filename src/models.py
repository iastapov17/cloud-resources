import datetime
import enum

from pydantic import BaseModel


class ResourceType(enum.Enum):
    DB = "db"
    VM = "vm"


class Price(BaseModel):
    id: int
    cost: int
    cpu: int
    id: int
    name: str
    ram: int
    type: ResourceType


class GetResource(BaseModel):
    id: int
    cost: int
    cpu: int
    cpu_load: float
    failed: bool
    failed_until: datetime.datetime
    ram: int
    ram_load: float
    type: ResourceType


class PostResource(BaseModel):
    cpu: int
    ram: int
    type: ResourceType


class Stat(BaseModel):
    availability: float
    cost_total: float
    db_cpu: float
    db_cpu_load: float
    db_ram: float
    db_ram_load: float
    last1: float
    last5: float
    last15: float
    cost_total: float
    lastDay: float
    lastHour: float
    lastWeek: float
    offline_time: float
    online: bool
    online_time: float
    requests: float
    requests_total: float
    response_time: float
    vm_cpu: float
    vm_cpu_load: float
    vm_ram: float
    vm_ram_load: float
    timestamp: datetime.datetime
