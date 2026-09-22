"""温室灌溉决策服务。

领域约定见 reference/domain.json：
- 时间一律使用带偏移量的 ISO 8601；
- 水量单位升，流量单位升/分钟；
- 遥测质量分 good / suspect / offline，接收顺序不等于发生顺序。
"""

from .config import Domain, ServiceConfig, load_config, load_domain
from .service import IrrigationService
from .store import EventStore

__all__ = [
    "IrrigationService",
    "EventStore",
    "Domain",
    "ServiceConfig",
    "load_domain",
    "load_config",
]
