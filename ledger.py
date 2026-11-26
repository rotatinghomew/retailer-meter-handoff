"""台账/对账入口的兼容模块名。"""

from service import (  # noqa: F401
    Api,
    Store,
    SwitchService as Ledger,
    SwitchService,
    ServiceError,
)

LedgerService = SwitchService
