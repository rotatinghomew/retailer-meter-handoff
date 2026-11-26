"""兼容性别名：核心实现位于 :mod:`service`。"""

from service import *  # noqa: F401,F403
from service import (  # noqa: F401
    Api,
    SwitchService,
    Store,
    build_http_server,
    actor_from_headers,
)

# 常见命名别名。
SwitchServiceService = SwitchService
SwitchingService = SwitchService
PowerSwitchService = SwitchService
Ledger = SwitchService
LedgerService = SwitchService
Service = SwitchService
create_service = SwitchService
create_app = build_http_server
