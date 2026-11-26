"""售电换签、计量交界、更正与对账服务。

仅使用 Python 标准库。核心对象可直接以 ``SwitchService`` 调用，也可通过
``python service.py --help`` 启动 JSON HTTP 服务。
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
CHINA_TZ = timezone(timedelta(hours=8))

# 业务状态常量。HTTP/调用方既可用英文 code，也可通过 state_label 看中文。
DRAFT = "draft"
DECLARED = "declared"
AGREEMENT_PENDING = "agreement_pending"
SCHEDULED = "scheduled"
APPLYING = "applying"
ACCEPTED = "accepted"
DELAYED = "delayed"
PARTIAL = "partially_applied"
NEEDS_DECISION = "partial_needs_decision"
BRIDGING = "bridging"
CONFIRMED = "confirmed"
NOT_SWITCHED = "not_switched"
WITHDRAWN = "withdrawn"
DECLINED = "declined"

STATE_LABELS = {
    DRAFT: "草稿",
    DECLARED: "已申报",
    AGREEMENT_PENDING: "待双方约定",
    SCHEDULED: "已约定切换日",
    APPLYING: "申报中",
    ACCEPTED: "计量方已受理",
    DELAYED: "延期/待确认",
    PARTIAL: "部分点已确认",
    NEEDS_DECISION: "部分通过，待客户决定剩余点",
    BRIDGING: "临时承接中",
    CONFIRMED: "已有确认/正式切换",
    NOT_SWITCHED: "未切换，仍由原方服务",
    WITHDRAWN: "已撤回",
    DECLINED: "已拒绝",
}


class ServiceError(Exception):
    status = 400
    code = "bad_request"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.message = message
        self.details = details or {}


class ValidationError(ServiceError):
    status = 422
    code = "validation_error"


class NotFoundError(ServiceError):
    status = 404
    code = "not_found"


class PermissionDeniedError(ServiceError):
    status = 403
    code = "permission_denied"


class ConflictError(ServiceError):
    status = 409
    code = "conflict"


class AttributionUndeterminedError(ConflictError):
    code = "attribution_undetermined"


def now_iso() -> str:
    return datetime.now(CHINA_TZ).isoformat(timespec="seconds")


def parse_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    else:
        raise ValidationError("时间必须是 ISO-8601 字符串")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=CHINA_TZ)
    return dt


def iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=CHINA_TZ)
    return value.astimezone(CHINA_TZ).isoformat(timespec="seconds")


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class Store:
    """把聚合 JSON 文档保存在 SQLite；重启后事实和幂等键仍在。"""

    def __init__(self, path: str | Path = ":memory:"):
        self.path = ":memory:" if path is None else str(path)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS docs (
                kind TEXT NOT NULL,
                id TEXT NOT NULL,
                data TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                PRIMARY KEY (kind, id)
            )
            """
        )
        self.db.execute(
            """
            CREATE TABLE IF NOT EXISTS idempotency (
                idempotency_key TEXT PRIMARY KEY,
                response TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        self.db.commit()

    def close(self) -> None:
        with self.lock:
            self.db.close()

    def get(self, kind: str, item_id: str) -> dict[str, Any] | None:
        with self.lock:
            row = self.db.execute(
                "SELECT data FROM docs WHERE kind=? AND id=?", (kind, item_id)
            ).fetchone()
            return json.loads(row["data"]) if row else None

    def require(self, kind: str, item_id: str) -> dict[str, Any]:
        value = self.get(kind, item_id)
        if value is None:
            raise NotFoundError(f"{kind}/{item_id} 不存在")
        return value

    def list(self, kind: str) -> list[dict[str, Any]]:
        with self.lock:
            rows = self.db.execute(
                "SELECT data FROM docs WHERE kind=? ORDER BY id", (kind,)
            ).fetchall()
            return [json.loads(row["data"]) for row in rows]

    def put(self, kind: str, item_id: str, value: dict[str, Any]) -> dict[str, Any]:
        text = json.dumps(value, ensure_ascii=False, sort_keys=True)
        with self.lock:
            self.db.execute(
                """
                INSERT INTO docs(kind,id,data,updated_at) VALUES(?,?,?,?)
                ON CONFLICT(kind,id) DO UPDATE SET data=excluded.data,
                    updated_at=excluded.updated_at
                """,
                (kind, item_id, text, now_iso()),
            )
            self.db.commit()
        return value

    def delete(self, kind: str, item_id: str) -> None:
        with self.lock:
            self.db.execute("DELETE FROM docs WHERE kind=? AND id=?", (kind, item_id))
            self.db.commit()

    def idempotent(self, key: str | None, producer: Callable[[], Any]) -> Any:
        if not key:
            return producer()
        with self.lock:
            row = self.db.execute(
                "SELECT response FROM idempotency WHERE idempotency_key=?", (key,)
            ).fetchone()
            if row:
                return json.loads(row["response"])
            result = producer()
            self.db.execute(
                "INSERT OR IGNORE INTO idempotency VALUES(?,?,?)",
                (key, json.dumps(result, ensure_ascii=False, sort_keys=True), now_iso()),
            )
            self.db.commit()
            return result


def normalize_actor(actor: Any = None) -> dict[str, Any]:
    if actor is None:
        return {"type": "system", "id": "local-system"}
    if isinstance(actor, str):
        if ":" in actor:
            kind, ident = actor.split(":", 1)
            return {"type": kind.strip(), "id": ident.strip()}
        return {"type": "customer", "id": actor}
    if isinstance(actor, dict):
        result = dict(actor)
        result.setdefault("id", result.get("name") or result.get("actor_id") or "unknown")
        if "type" not in result:
            result["type"] = result.get("actor_type", "customer")
        return result
    raise ValidationError("actor 必须是字符串或对象")


class SwitchService:
    """售电交接应用服务。

    所有写操作均幂等（相同事件/读数/请求号重放不会产生第二个交界或账单）。
    ``actor=None`` 表示受信任的本地调用；HTTP 层会从请求头构造身份。
    """

    def __init__(
        self,
        db_path: str | Path | None = ":memory:",
        *,
        data_dir: str | Path | None = None,
        peer_url: str | None = None,
    ):
        # 便利不同调用习惯：把已存在的数据目录作为第一个位置参数时，视为 data_dir。
        if db_path not in (None, ":memory:") and data_dir is None:
            candidate = Path(db_path)
            if candidate.is_dir() and (candidate / "meters.json").exists():
                data_dir = candidate
                db_path = ":memory:"
        self.store = Store(db_path or ":memory:")
        # 常见命名别名，便于调用方直接访问 SQLite 连接。
        self.db = self.store.db
        self.conn = self.store.db
        self.peer_url = peer_url
        if data_dir is not None:
            self.seed_directory(data_dir)

    # ------------------------------------------------------------------
    # 初始化、资料与权限
    # ------------------------------------------------------------------
    def seed_directory(self, directory: str | Path) -> dict[str, int]:
        path = Path(directory)
        counts: dict[str, int] = {}

        meters = self._read_json(path / "meters.json")
        for meter in meters:
            self.upsert_meter(meter)
        counts["meters"] = len(meters)

        tariffs = self._read_json(path / "tariffs.json")
        for tariff in tariffs:
            self.store.put("tariff", tariff["id"], tariff)
        counts["tariffs"] = len(tariffs)

        # 先把申报单作为“客户已确认且双方已按申报日约定”的事实种子。
        requests = self._read_json(path / "requests.json")
        for row in requests:
            if not self.store.get("switch", row["id"]):
                payload = dict(row)
                payload["agreed"] = True
                payload["seeded"] = True
                self.create_switch(payload, actor={"type": "system", "id": "seed"})
        counts["requests"] = len(requests)

        reads = self._read_json(path / "reads.json")
        for reading in reads:
            self.ingest_reading(reading, actor={"type": "meter", "id": "seed"})
        counts["reads"] = len(reads)
        return counts

    @staticmethod
    def _read_json(path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, list):
            raise ValidationError(f"{path.name} 必须是 JSON 数组")
        return value

    def upsert_meter(self, meter: dict[str, Any]) -> dict[str, Any]:
        if "id" not in meter:
            raise ValidationError("计量点缺少 id")
        existing = self.store.get("meter", meter["id"]) or {}
        merged = {**existing, **meter}
        self.store.put("meter", meter["id"], merged)
        return merged

    def register_master_agreement(
        self, agreement: dict[str, Any], actor: Any = None
    ) -> dict[str, Any]:
        actor = normalize_actor(actor)
        agreement_id = agreement.get("id") or agreement.get("agreement_id") or _new_id("MA")
        customer = agreement.get("customer") or agreement.get("company")
        meter_ids = list(agreement.get("meter_ids") or agreement.get("meters") or [])
        if not customer or not meter_ids:
            raise ValidationError("总协议必须包含 customer/company 和 meter_ids")
        self.require_customer(customer, actor)
        for mid in meter_ids:
            meter = self.store.require("meter", mid)
            if meter.get("company") != customer:
                raise ValidationError(f"{mid} 不属于 {customer}")
        doc = {
            "id": agreement_id,
            "customer": customer,
            "meter_ids": meter_ids,
            "groups": agreement.get("groups") or [],
            "terms": agreement.get("terms") or {},
            "created_at": now_iso(),
        }
        return self.store.put("agreement", agreement_id, doc)

    def _actor_allowed_for_customer(self, customer: str, actor: dict[str, Any]) -> bool:
        if actor["type"] in {"system", "admin"}:
            return True
        if actor["type"] == "customer":
            return actor.get("id") == customer or actor.get("customer") == customer
        return False

    def require_customer(self, customer: str, actor: Any) -> dict[str, Any]:
        actor = normalize_actor(actor)
        if not self._actor_allowed_for_customer(customer, actor):
            raise PermissionDeniedError(f"无权操作 {customer} 的换签")
        return actor

    def require_retailer(self, retailer: str, actor: Any) -> dict[str, Any]:
        actor = normalize_actor(actor)
        if actor["type"] in {"system", "admin"}:
            return actor
        if actor["type"] in {"retailer", "finance"}:
            ident = actor.get("retailer") or actor.get("id")
            if ident == retailer:
                return actor
        raise PermissionDeniedError(f"财务/售方身份无权访问 {retailer} 的资料")

    def require_meter_operator(self, actor: Any) -> dict[str, Any]:
        actor = normalize_actor(actor)
        if actor["type"] not in {"system", "admin", "meter"}:
            raise PermissionDeniedError("只有计量方可以回告计量事件/读数")
        return actor

    # ------------------------------------------------------------------
    # 换签申报、权限/范围/边界核对
    # ------------------------------------------------------------------
    def create_switch(
        self,
        payload: dict[str, Any],
        actor: Any = None,
        *,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        actor = normalize_actor(actor)
        key = idempotency_key or payload.get("idempotency_key")

        def produce() -> dict[str, Any]:
            return self._create_switch_once(payload, actor)

        return self.store.idempotent(key, produce)

    # 语义化别名，方便不同调用方/测试命名习惯。
    confirm_switch = create_switch
    declare_switch = create_switch
    create_change = create_switch
    create_request = create_switch

    def _create_switch_once(
        self, payload: dict[str, Any], actor: dict[str, Any]
    ) -> dict[str, Any]:
        cid = str(
            payload.get("id")
            or payload.get("change_id")
            or payload.get("request_id")
            or _new_id("CH")
        )
        customer = payload.get("company") or payload.get("customer")
        out = payload.get("out") or payload.get("out_retailer") or payload.get("current_retailer")
        incoming = payload.get("incoming") or payload.get("incoming_retailer") or payload.get("new_retailer")
        planned_raw = payload.get("planned") or payload.get("planned_at") or payload.get("plan_time")
        group = payload.get("group") or payload.get("mode") or "separate"
        meter_ids = list(payload.get("meter_ids") or payload.get("meters") or [])

        if not customer:
            raise ValidationError("缺少 company/customer")
        self.require_customer(customer, actor)
        if not out or not incoming:
            raise ValidationError("必须提供 out 和 incoming 售电公司")
        if out == incoming:
            raise ValidationError("接方与当前售方不能相同")
        if group not in {"both", "separate"}:
            raise ValidationError("group 只能是 both 或 separate")
        if not planned_raw:
            raise ValidationError("缺少申报计划切换日 planned/planned_at")
        planned_dt = parse_dt(planned_raw)
        planned = iso(planned_dt)

        # 总协议选择边界。
        agreement_id = payload.get("master_agreement_id") or payload.get("agreement_id")
        if agreement_id:
            agreement = self.store.require("agreement", agreement_id)
            if agreement["customer"] != customer:
                raise PermissionDeniedError("总协议不属于该客户")
            group_name = payload.get("agreement_group")
            if group_name:
                groups = {g.get("name"): g.get("meter_ids", []) for g in agreement.get("groups", [])}
                if group_name not in groups:
                    raise ValidationError(f"总协议中没有计量组 {group_name}")
                agreement_meters = groups[group_name]
            else:
                agreement_meters = agreement["meter_ids"]
            if payload.get("all_agreement_meters") or not meter_ids:
                meter_ids = list(agreement_meters)
            elif payload.get("whole_agreement_group"):
                if set(meter_ids) != set(agreement_meters):
                    raise ValidationError("whole_agreement_group 要求选择整组计量点")
            elif not set(meter_ids).issubset(set(agreement_meters)):
                raise ValidationError("所选计量点超出客户总协议边界")

        if not meter_ids:
            raise ValidationError("至少选择一个计量点")
        if len(meter_ids) != len(set(meter_ids)):
            raise ValidationError("meter_ids 不能重复")

        signature_meters = list(meter_ids)
        existing = self.store.get("switch", cid)
        if existing:
            same = (
                existing["customer"] == customer
                and existing["out"] == out
                and existing["incoming"] == incoming
                and existing["meter_ids"] == signature_meters
                and existing["planned"] == planned
            )
            if same:
                return self.get_switch(cid)
            raise ConflictError(
                f"换签号 {cid} 已被不同申报内容占用",
                details={"existing": {"meter_ids": existing["meter_ids"], "planned": existing["planned"]}},
            )

        meters: list[dict[str, Any]] = []
        for mid in meter_ids:
            meter = self.store.get("meter", mid)
            if meter is None:
                raise NotFoundError(f"计量点 {mid} 不存在")
            if meter.get("company") != customer:
                raise ValidationError(f"计量点 {mid} 不属于 {customer}")
            # 注意：meter.current 是资料中的初始责任方；经过已确认交界后，
            # 当前有效责任以 resolve_attribution 的商业归属结果为准。
            meters.append(meter)

        # 接方服务范围：样例中用 tariff.zones 表达可供范围。
        for meter in meters:
            zone = meter.get("zone")
            in_scope = any(
                t.get("retailer") == incoming and (not t.get("zones") or zone in t.get("zones", []))
                for t in self.store.list("tariff")
            )
            scopes = self.store.list("service_scope")
            in_scope = in_scope or any(
                s.get("retailer") == incoming
                and (not s.get("zones") or zone in s.get("zones", []))
                and (not s.get("meter_ids") or mid in s.get("meter_ids", []))
                for s in scopes
            )
            if not in_scope:
                raise ValidationError(f"接方 {incoming} 不在 {mid}/{zone} 的服务范围内")

        bridge_terms = payload.get("bridge_terms")
        if group == "both":
            if not bridge_terms:
                raise ValidationError("both 组必须预先确认 bridge_terms 临时承接条款")
            if not bridge_terms.get("owner"):
                raise ValidationError("bridge_terms.owner 不能为空")
            try:
                cents = int(bridge_terms.get("cents_per_kwh"))
                if cents < 0:
                    raise ValueError
            except (TypeError, ValueError):
                raise ValidationError("bridge_terms.cents_per_kwh 必须是非负整数分/kWh")

        # 查前一个有效归属。未确定时仍允许留档申请，但不能发出新承诺/向计量方提交。
        attribution_results = {
            mid: self.resolve_attribution(mid, planned, create_if_missing=False)
            for mid in meter_ids
        }
        undetermined = {
            mid: item for mid, item in attribution_results.items() if item["status"] != "determined"
        }
        attribution_status = "undetermined" if undetermined else "determined"
        if not undetermined:
            for mid, attribution in attribution_results.items():
                owner = attribution.get("owner")
                if owner and owner != out:
                    raise ConflictError(
                        f"{mid} 在 {planned} 的有效归属是 {owner}，不能以 {out} 为起点发新承诺",
                        details={"attribution": attribution},
                    )
                latest = self.latest_boundary(mid)
                if latest and planned_dt <= parse_dt(latest["at"]):
                    raise ConflictError(
                        f"新合同起点 {planned} 不能早于或等于 {mid} 已确认交界 {latest['at']}；"
                        "只能在该交界以后重新判断",
                        details={"boundary_id": latest["id"]},
                    )

        agreed = bool(payload.get("agreed") or payload.get("agreement_status") == "agreed")
        timestamp = payload.get("created_at") or now_iso()
        consents = {
            "customer": payload.get("customer_agreed_at") or timestamp,
            "out": payload.get("out_agreed_at") or (payload.get("agreed_at") or timestamp if agreed else None),
            "incoming": payload.get("incoming_agreed_at") or (payload.get("agreed_at") or timestamp if agreed else None),
            "agreed_at": payload.get("agreed_at") or (timestamp if agreed else None),
        }
        commitment_allowed = attribution_status == "determined"
        doc: dict[str, Any] = {
            "id": cid,
            "change_id": cid,
            "customer": customer,
            "company": customer,
            "out": out,
            "out_retailer": out,
            "incoming": incoming,
            "incoming_retailer": incoming,
            "group": group,
            "mode": group,
            "meter_ids": signature_meters,
            "planned": planned,
            "planned_at": planned,
            "bridge_terms": bridge_terms,
            "master_agreement_id": agreement_id,
            "agreement": consents,
            "agreement_status": "agreed" if consents["agreed_at"] else "pending",
            "attribution_status": attribution_status,
            "attribution_checks": attribution_results,
            "commitment_issued": bool(agreed and commitment_allowed),
            "commitment_allowed": commitment_allowed,
            "commitment_at": (consents["agreed_at"] if agreed and commitment_allowed else None),
            "submitted_at": None,
            "withdrawal": None,
            "decisions": {},
            "continuations": [],
            "continuation_of": payload.get("continuation_of"),
            "completes_group": payload.get("completes_group"),
            "peer_apply_path": payload.get("peer_apply_path") or f"/switches/{cid}",
            "peer_withdraw_path": payload.get("peer_withdraw_path") or self._default_withdraw_path(cid),
            "outbound": [],
            "point_states": {mid: {"state": DECLARED, "events": []} for mid in meter_ids},
            "created_at": timestamp,
            "updated_at": timestamp,
            "seeded": bool(payload.get("seeded")),
        }
        self.store.put("switch", cid, doc)
        if doc["completes_group"]:
            self._refresh_switch(doc["completes_group"])
        return self.get_switch(cid)

    @staticmethod
    def _default_withdraw_path(cid: str) -> str:
        # 联调脚本 CH-14 使用 /switches/cancel-14。
        if cid.startswith("CH-") and cid[3:].isdigit():
            return f"/switches/cancel-{cid[3:]}"
        return f"/switches/{cid}/withdraw"

    def agree(
        self,
        switch_id: str,
        actor: Any = None,
        *,
        party: str | None = None,
        at: str | datetime | None = None,
    ) -> dict[str, Any]:
        sw = self.store.require("switch", switch_id)
        actor = normalize_actor(actor)
        at_iso = iso(parse_dt(at)) if at else now_iso()

        if actor["type"] == "customer" or party == "customer":
            self.require_customer(sw["customer"], actor)
            sw["agreement"]["customer"] = at_iso
        elif actor["type"] in {"retailer", "finance"} or party in {"out", "incoming"}:
            ident = actor.get("retailer") or actor.get("id")
            if party == "out" or ident == sw["out"]:
                self.require_retailer(sw["out"], actor)
                sw["agreement"]["out"] = at_iso
            elif party == "incoming" or ident == sw["incoming"]:
                self.require_retailer(sw["incoming"], actor)
                sw["agreement"]["incoming"] = at_iso
            else:
                raise PermissionDeniedError("该售方不是本单的旧方或接方")
        elif actor["type"] in {"system", "admin"}:
            key = party or "agreed_at"
            if key in {"out", "incoming", "customer"}:
                sw["agreement"][key] = at_iso
            else:
                sw["agreement"]["out"] = at_iso
                sw["agreement"]["incoming"] = at_iso
        else:
            raise PermissionDeniedError("无权确认约定")

        if sw["agreement"].get("out") and sw["agreement"].get("incoming") and sw["agreement"].get("customer"):
            sw["agreement"]["agreed_at"] = at_iso
            sw["agreement_status"] = "agreed"
            # 双方约定日是商业承诺；若前序归属仍未查清，不能发出以错误售方为起点的承诺。
            unresolved = []
            for mid in sw["meter_ids"]:
                attribution = self.resolve_attribution(
                    mid, sw["planned"], exclude_switch_id=switch_id
                )
                sw["attribution_checks"][mid] = attribution
                if attribution["status"] != "determined":
                    unresolved.append(mid)
            sw["attribution_status"] = "undetermined" if unresolved else "determined"
            sw["commitment_allowed"] = not unresolved
            if not unresolved:
                sw["commitment_issued"] = True
                sw["commitment_at"] = at_iso
            else:
                sw["commitment_issued"] = False
        sw["updated_at"] = now_iso()
        self.store.put("switch", switch_id, sw)
        return self.get_switch(switch_id)

    def issue_commitment(self, switch_id: str, actor: Any = None) -> dict[str, Any]:
        sw = self.store.require("switch", switch_id)
        self.require_customer(sw["customer"], actor)
        for mid in sw["meter_ids"]:
            result = self.resolve_attribution(
                mid, sw["planned"], exclude_switch_id=switch_id
            )
            sw["attribution_checks"][mid] = result
            if result["status"] != "determined":
                raise AttributionUndeterminedError(
                    f"{mid} 的前一个有效归属尚未确定，不能发出新承诺",
                    details={"attribution": result},
                )
            if result.get("owner") != sw["out"]:
                raise ConflictError(
                    f"{mid} 的有效归属为 {result.get('owner')}，与申报旧方 {sw['out']} 不一致"
                )
        sw["attribution_status"] = "determined"
        sw["commitment_allowed"] = True
        sw["commitment_issued"] = True
        sw["commitment_at"] = now_iso()
        self.store.put("switch", switch_id, sw)
        return self.get_switch(switch_id)

    # ------------------------------------------------------------------
    # 计量事件、乱序/重复/撤回交叉处理
    # ------------------------------------------------------------------
    def ingest_events(
        self,
        payload: dict[str, Any] | list[dict[str, Any]],
        actor: Any = None,
        *,
        context: str = "switch",
        source_path: str | None = None,
    ) -> dict[str, Any]:
        self.require_meter_operator(actor)
        if isinstance(payload, list):
            events = payload
        elif isinstance(payload, dict):
            events = payload.get("events") or []
        else:
            raise ValidationError("事件报文必须是对象或数组")

        processed: list[str] = []
        duplicates: list[str] = []
        ignored: list[dict[str, Any]] = []
        affected: set[str] = set()

        for raw in events:
            cid = raw.get("change_id")
            mid = raw.get("meter_id")
            status = raw.get("status")
            if not cid or not mid or not status:
                ignored.append({"event": raw, "reason": "缺少 change_id/meter_id/status"})
                continue
            sw = self.store.get("switch", cid)
            if sw is None:
                ignored.append({"event": raw, "reason": "未知 change_id"})
                continue
            if mid not in sw["meter_ids"]:
                ignored.append({"event": raw, "reason": "事件计量点不属于本换签边界"})
                continue

            event_context = raw.get("context") or context
            # withdrawn 与 already-applied 拒绝都属于撤回侧事件；即使调用方未显式传 context 也不能误判。
            if status in {"withdrawn", "withdrawal_accepted"}:
                event_context = "withdrawal"
            if status == "declined" and raw.get("reason") == "already-applied":
                event_context = "withdrawal"
            at = raw.get("at") or now_iso()
            revision = int(raw.get("revision") or 0)
            event_id = str(
                raw.get("event_id")
                or raw.get("id")
                or f"{cid}:{mid}:{revision}:{status}:{iso(parse_dt(at))}:{event_context}"
            )
            if self.store.get("meter_event", event_id):
                duplicates.append(event_id)
                continue

            event = {
                "id": event_id,
                "change_id": cid,
                "meter_id": mid,
                "status": status,
                "at": iso(parse_dt(at)),
                "revision": revision,
                "reason": raw.get("reason"),
                "reading_id": raw.get("reading_id"),
                "context": event_context,
                "source_path": source_path,
                "received_at": now_iso(),
                "raw": raw,
            }
            self.store.put("meter_event", event_id, event)
            processed.append(event_id)
            affected.add(cid)

        for cid in affected:
            self._refresh_switch(cid)
            sw = self.store.get("switch", cid)
            if sw.get("completes_group"):
                self._refresh_switch(sw["completes_group"])

        return {"processed": processed, "duplicates": duplicates, "ignored": ignored}

    handle_meter_events = ingest_events
    handle_peer_response = ingest_events
    process_events = ingest_events
    receive_events = ingest_events

    def _events_for_point(self, cid: str, mid: str) -> list[dict[str, Any]]:
        events = [
            e
            for e in self.store.list("meter_event")
            if e["change_id"] == cid and e["meter_id"] == mid
        ]
        return sorted(events, key=lambda e: (parse_dt(e["at"]), e["revision"], e["id"]))

    def _reduce_point(self, cid: str, mid: str) -> dict[str, Any]:
        events = self._events_for_point(cid, mid)
        accepted_event: dict[str, Any] | None = None
        terminal_candidates: list[tuple[str, dict[str, Any]]] = []
        withdrawal_rejected: dict[str, Any] | None = None
        latest_withdrawal: dict[str, Any] | None = None

        for event in events:
            status = event["status"]
            if event["context"] == "withdrawal":
                if status in {"withdrawn", "withdrawal_accepted"}:
                    terminal_candidates.append(("withdrawn", event))
                    latest_withdrawal = event
                elif status == "declined" and event.get("reason") == "already-applied":
                    withdrawal_rejected = event
                continue

            if status == "accepted":
                accepted_event = event
            elif status == "applied":
                terminal_candidates.append(("applied", event))
            elif status == "declined":
                terminal_candidates.append(("declined", event))

        result: dict[str, Any] = {
            "state": DECLARED,
            "effective_at": None,
            "event_id": None,
            "reading_id": None,
            "reason": None,
            "withdrawal": latest_withdrawal,
            "withdrawal_rejected": withdrawal_rejected,
        }

        if terminal_candidates:
            terminal_type, event = min(
                terminal_candidates,
                key=lambda item: (parse_dt(item[1]["at"]), item[1]["revision"], item[1]["id"]),
            )
            if terminal_type == "applied":
                result.update(
                    state=CONFIRMED,
                    effective_at=event["at"],
                    event_id=event["id"],
                    reading_id=event.get("reading_id"),
                )
            elif terminal_type == "declined":
                result.update(state=DECLINED, event_id=event["id"], reason=event.get("reason"))
            else:
                result.update(state=WITHDRAWN, event_id=event["id"], effective_at=event["at"])
        elif withdrawal_rejected:
            # 计量方明确回复 already-applied，也是“撤回前已生效”的权威证据。
            sw = self.store.require("switch", cid)
            result.update(
                state=CONFIRMED,
                effective_at=sw["planned"],
                event_id=withdrawal_rejected["id"],
                reason="withdrawal_rejected_already_applied",
            )
        elif accepted_event:
            result.update(state=ACCEPTED, event_id=accepted_event["id"])

        # 已完成事实不能被后到的旧 accepted 或重复报文推翻；上面的终局排序已保证。
        return result

    def _refresh_switch(self, cid: str, *, now: datetime | None = None) -> dict[str, Any]:
        sw = self.store.require("switch", cid)
        now = now or datetime.now(CHINA_TZ)
        point_states: dict[str, dict[str, Any]] = {}
        applied: list[str] = []
        failed: list[str] = []
        pending: list[str] = []

        for mid in sw["meter_ids"]:
            reduced = self._reduce_point(cid, mid)
            if reduced["state"] == CONFIRMED:
                boundary = self._upsert_boundary_from_point(sw, mid, reduced)
                reduced["meter_state"] = CONFIRMED
                reduced["physical_state"] = CONFIRMED
                formal_from = boundary.get("formal_from")
                in_bridge = boundary.get("commercial_mode") == "bridge" and (
                    not formal_from or now < parse_dt(formal_from)
                )
                reduced["commercial_state"] = BRIDGING if in_bridge else CONFIRMED
                reduced["business_state"] = BRIDGING if in_bridge else CONFIRMED
            point_states[mid] = reduced
            if reduced["state"] == CONFIRMED:
                applied.append(mid)
            elif reduced["state"] in {DECLINED, WITHDRAWN}:
                failed.append(mid)
            else:
                pending.append(mid)

        sw["point_states"] = point_states
        sw["applied_meter_ids"] = applied
        sw["failed_meter_ids"] = failed
        sw["pending_meter_ids"] = pending

        # 处理 continuation 成功后补齐 both 组。
        continuation_ids = sw.get("continuations", [])
        continuation_applied: set[str] = set()
        for follow_id in continuation_ids:
            follow = self.store.get("switch", follow_id)
            if follow:
                continuation_applied.update(follow.get("applied_meter_ids", []))
        all_group_success = set(applied) | continuation_applied

        if sw["group"] == "both" and len(all_group_success) == len(sw["meter_ids"]):
            effective_times = []
            for mid in sw["meter_ids"]:
                boundary = self._boundary_for_change_meter(cid, mid)
                if boundary:
                    effective_times.append(parse_dt(boundary["at"]))
                else:
                    follow = self._continuation_for_meter(sw, mid)
                    if follow:
                        b = self._boundary_for_change_meter(follow["id"], mid)
                        if b:
                            effective_times.append(parse_dt(b["at"]))
            formal_at = iso(max(effective_times)) if effective_times else sw["planned"]
            for mid in applied:
                boundary = self._boundary_for_change_meter(cid, mid)
                if boundary and not boundary.get("formal_from"):
                    boundary["formal_from"] = formal_at
                    boundary["formal_commercial_owner"] = sw["incoming"]
                    self.store.put("boundary", boundary["id"], boundary)
            for follow_id in continuation_ids:
                follow = self.store.get("switch", follow_id)
                if not follow:
                    continue
                for mid in follow.get("applied_meter_ids", []):
                    boundary = self._boundary_for_change_meter(follow_id, mid)
                    if boundary and not boundary.get("formal_from"):
                        boundary["formal_from"] = formal_at
                        boundary["formal_commercial_owner"] = sw["incoming"]
                        self.store.put("boundary", boundary["id"], boundary)
            sw["formal_commercial_at"] = formal_at
            sw["commercial_state"] = "formal"
            sw["state"] = CONFIRMED
            sw["status"] = CONFIRMED
        elif applied:
            if sw["group"] == "both":
                sw["commercial_state"] = BRIDGING
                unresolved = [m for m in sw["meter_ids"] if m not in all_group_success]
                decided = all(m in sw.get("decisions", {}) for m in unresolved)
                if failed and not decided:
                    sw["state"] = NEEDS_DECISION
                    sw["status"] = NEEDS_DECISION
                elif sw.get("remaining_action") == "stop" or (failed and decided):
                    sw["state"] = BRIDGING
                    sw["status"] = BRIDGING
                else:
                    sw["state"] = PARTIAL
                    sw["status"] = PARTIAL
            else:
                sw["commercial_state"] = "formal"
                sw["state"] = PARTIAL if pending or failed else CONFIRMED
                sw["status"] = sw["state"]
        elif sw["group"] == "both" and failed and pending:
            unresolved = [m for m in sw["meter_ids"] if m in failed or m in pending]
            decided = all(m in sw.get("decisions", {}) for m in unresolved)
            sw["state"] = NEEDS_DECISION if failed and not decided else PARTIAL
            sw["status"] = sw["state"]
        elif failed and not pending:
            sw["state"] = NOT_SWITCHED
            sw["status"] = NOT_SWITCHED
        elif any(p.get("state") == ACCEPTED for p in point_states.values()) or sw.get("submitted_at"):
            planned_dt = parse_dt(sw["planned"])
            if now > planned_dt and not applied:
                sw["state"] = DELAYED
                sw["status"] = DELAYED
            elif any(p.get("state") == ACCEPTED for p in point_states.values()):
                sw["state"] = ACCEPTED
                sw["status"] = ACCEPTED
            else:
                sw["state"] = APPLYING
                sw["status"] = APPLYING
        elif sw.get("agreement_status") == "agreed":
            sw["state"] = SCHEDULED
            sw["status"] = SCHEDULED
        else:
            sw["state"] = AGREEMENT_PENDING
            sw["status"] = AGREEMENT_PENDING

        if not applied:
            sw["commercial_state"] = "original_contract"

        # 每次读取/重放后重新核查前序归属；障碍消失且已有双方约定时，允许补发承诺。
        unresolved_attribution = []
        for mid in sw["meter_ids"]:
            attribution = self.resolve_attribution(
                mid, sw["planned"], exclude_switch_id=cid
            )
            sw.setdefault("attribution_checks", {})[mid] = attribution
            if attribution["status"] != "determined":
                unresolved_attribution.append(mid)
        sw["attribution_status"] = "undetermined" if unresolved_attribution else "determined"
        sw["commitment_allowed"] = not unresolved_attribution
        if (
            not unresolved_attribution
            and sw.get("agreement_status") == "agreed"
            and not sw.get("commitment_issued")
        ):
            sw["commitment_issued"] = True
            sw["commitment_at"] = sw.get("commitment_at") or now_iso()

        sw["state_label"] = STATE_LABELS.get(sw["state"], sw["state"])
        sw["updated_at"] = now_iso()
        self.store.put("switch", cid, sw)
        return sw

    def _continuation_for_meter(self, root: dict[str, Any], mid: str) -> dict[str, Any] | None:
        for cid in root.get("continuations", []):
            follow = self.store.get("switch", cid)
            if follow and mid in follow.get("meter_ids", []):
                return follow
        return None

    def _boundary_for_change_meter(self, cid: str, mid: str) -> dict[str, Any] | None:
        boundary_id = f"BND-{cid}-{mid}"
        return self.store.get("boundary", boundary_id)

    def _upsert_boundary_from_point(
        self, sw: dict[str, Any], mid: str, point: dict[str, Any]
    ) -> dict[str, Any]:
        cid = sw["id"]
        boundary_id = f"BND-{cid}-{mid}"
        boundary = self.store.get("boundary", boundary_id) or {
            "id": boundary_id,
            "change_id": cid,
            "meter_id": mid,
            "created_at": now_iso(),
        }
        at = point.get("effective_at") or sw["planned"]
        boundary.update(
            {
                "at": at,
                "effective_at": at,
                "status": "active",
                "physical_confirmed": True,
                "source_event_id": point.get("event_id"),
                "before_owner": sw["out"],
                "old_retailer": sw["out"],
                "incoming_retailer": sw["incoming"],
            }
        )

        if sw["group"] == "both" and sw.get("bridge_terms"):
            boundary["commercial_mode"] = "bridge"
            boundary["commercial_owner"] = sw["bridge_terms"]["owner"]
            boundary["bridge_owner"] = sw["bridge_terms"]["owner"]
            boundary["cents_per_kwh"] = int(sw["bridge_terms"]["cents_per_kwh"])
            boundary["rate_kind"] = "bridge"
            boundary["formal_commercial_owner"] = sw["incoming"]
            boundary["formal_from"] = boundary.get("formal_from")
        else:
            boundary["commercial_mode"] = "formal"
            boundary["commercial_owner"] = sw["incoming"]
            boundary["formal_commercial_owner"] = sw["incoming"]
            boundary["formal_from"] = at
            boundary["rate_kind"] = "tariff"

        reading = None
        if point.get("reading_id"):
            reading = self.store.get("reading", point["reading_id"])
        if reading is None:
            canonical_at_time = [
                r
                for r in self.canonical_readings(mid)
                if parse_dt(r["time"]) == parse_dt(at)
            ]
            reading = canonical_at_time[-1] if canonical_at_time else None
        if reading:
            boundary["reading_id"] = reading["id"]
            boundary["recognized_reading_id"] = self.current_reading_id(mid, reading["time"]) or reading["id"]
            current = self.store.get("reading", boundary["recognized_reading_id"]) or reading
            boundary["kwh"] = current["kwh"]
            boundary["reading_quality"] = current.get("quality")
            boundary["revision"] = current.get("revision", 1)
        else:
            boundary["reading_id"] = point.get("reading_id")
            boundary["reading_quality"] = None

        self.store.put("boundary", boundary_id, boundary)
        return boundary

    def decide_remaining(
        self,
        switch_id: str,
        decision: str,
        *,
        meter_ids: list[str] | None = None,
        note: str | None = None,
        create_followup: bool = False,
        planned: str | None = None,
        incoming: str | None = None,
        actor: Any = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        return self.store.idempotent(
            idempotency_key,
            lambda: self._decide_remaining_once(
                switch_id,
                decision,
                meter_ids=meter_ids,
                note=note,
                create_followup=create_followup,
                planned=planned,
                incoming=incoming,
                actor=actor,
            ),
        )

    def _decide_remaining_once(self, switch_id, decision, *, meter_ids, note, create_followup, planned, incoming, actor):
        sw = self.store.require("switch", switch_id)
        self.require_customer(sw["customer"], actor)
        if decision not in {"continue", "stop"}:
            raise ValidationError("decision 只能是 continue 或 stop")
        unresolved = [
            mid
            for mid in sw["meter_ids"]
            if self._reduce_point(switch_id, mid)["state"] != CONFIRMED
        ]
        targets = meter_ids or unresolved
        for mid in targets:
            if mid not in unresolved:
                raise ValidationError(f"{mid} 不是未通过/未切换的剩余计量点")
            sw.setdefault("decisions", {})[mid] = {
                "decision": decision,
                "note": note,
                "at": now_iso(),
                "by": normalize_actor(actor).get("id"),
            }
        if decision == "stop" and set(targets) >= set(unresolved):
            sw["remaining_action"] = "stop"
        elif decision == "continue":
            sw["remaining_action"] = "continue"

        followup = None
        if decision == "continue" and create_followup:
            if not planned:
                raise ValidationError("继续办理需要新的 planned 时间")
            seq = len(sw.get("continuations", [])) + 1
            follow_id = f"{switch_id}-C{seq}"
            payload = {
                "id": follow_id,
                "company": sw["customer"],
                "out": sw["out"],
                "incoming": incoming or sw["incoming"],
                "group": "separate",
                "meter_ids": targets,
                "planned": planned,
                "bridge_terms": sw.get("bridge_terms"),
                "continuation_of": switch_id,
                "completes_group": switch_id,
                "agreed": True,
            }
            followup = self.create_switch(payload, actor={"type": "system", "id": "continuation"})
            sw.setdefault("continuations", []).append(follow_id)
        sw["updated_at"] = now_iso()
        self.store.put("switch", switch_id, sw)
        self._refresh_switch(switch_id)
        return {"switch": self.get_switch(switch_id), "followup": followup}

    # ------------------------------------------------------------------
    # 撤回与外方 HTTP 联调
    # ------------------------------------------------------------------
    def request_withdrawal(
        self,
        switch_id: str,
        meter_ids: list[str] | None = None,
        *,
        actor: Any = None,
        reason: str | None = None,
        call_peer: bool = True,
    ) -> dict[str, Any]:
        sw = self.store.require("switch", switch_id)
        self.require_customer(sw["customer"], actor)
        targets = meter_ids or list(sw["meter_ids"])
        if set(targets) - set(sw["meter_ids"]):
            raise ValidationError("撤回计量点超出本单边界")
        withdrawal = {
            "requested_at": now_iso(),
            "meter_ids": targets,
            "reason": reason,
            "status": "requested",
        }
        sw["withdrawal"] = withdrawal
        payload = {"change_id": switch_id, "meter_ids": targets, "action": "withdraw"}
        sw.setdefault("outbound", []).append(
            {"at": now_iso(), "path": sw["peer_withdraw_path"], "payload": payload}
        )
        self.store.put("switch", switch_id, sw)

        peer_result = None
        if call_peer and self.peer_url:
            peer_result = self.call_peer_withdrawal(switch_id, payload, sw["peer_withdraw_path"])
        return self.get_switch(switch_id) if peer_result is None else peer_result

    withdraw_switch = request_withdrawal

    def submit_to_meter(
        self,
        switch_id: str,
        actor: Any = None,
        *,
        call_peer: bool = True,
        poll: bool = False,
    ) -> dict[str, Any]:
        sw = self.store.require("switch", switch_id)
        self.require_customer(sw["customer"], actor)
        if not sw.get("commitment_allowed", True):
            raise AttributionUndeterminedError(
                "前序归属未确定，不能向计量方/新售方发出错误起点的承诺"
            )
        payload = {"meter_ids": list(sw["meter_ids"]), "action": "apply"}
        sw["submitted_at"] = sw.get("submitted_at") or now_iso()
        sw.setdefault("outbound", []).append(
            {"at": now_iso(), "path": sw["peer_apply_path"], "payload": payload}
        )
        self.store.put("switch", switch_id, sw)
        if call_peer and self.peer_url:
            body = self.peer_post(sw["peer_apply_path"], payload)
            if body is not None:
                self.ingest_events(body, actor={"type": "meter", "id": "peer"}, context="switch")
            if poll:
                return self.poll_meter(switch_id)
        return self.get_switch(switch_id)

    apply_to_meter = submit_to_meter

    def poll_meter(self, switch_id: str, *, actor: Any = None) -> dict[str, Any]:
        sw = self.store.require("switch", switch_id)
        if not self.peer_url:
            raise ValidationError("未配置 peer_url")
        body = self.peer_get(sw["peer_apply_path"])
        self.ingest_events(body, actor={"type": "meter", "id": "peer"}, context="switch")
        return self.get_switch(switch_id)

    def call_peer_withdrawal(self, switch_id: str, payload: dict, path: str) -> dict[str, Any]:
        try:
            body = self.peer_post(path, payload)
        except PeerDroppedConnection:
            body = None
        if body is None:
            # drop_first_reply：请求可能已送达，用 GET 取当前/最终回执。
            body = self.peer_get(path)
        self.ingest_events(
            body, actor={"type": "meter", "id": "peer"}, context="withdrawal", source_path=path
        )
        return self.get_switch(switch_id)

    def peer_post(self, path: str, payload: dict[str, Any]) -> dict[str, Any] | None:
        assert self.peer_url
        url = self.peer_url.rstrip("/") + path
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(url, data=data, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urlopen(request, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except URLError as exc:
            reason = getattr(exc, "reason", exc)
            if isinstance(reason, (ConnectionResetError, ConnectionAbortedError)) or "RemoteDisconnected" in type(reason).__name__:
                raise PeerDroppedConnection(str(reason))
            raise
        except Exception as exc:  # http.client.RemoteDisconnected 可能不是 URLError
            if "RemoteDisconnected" in type(exc).__name__:
                raise PeerDroppedConnection(str(exc))
            raise

    def peer_get(self, path: str) -> dict[str, Any]:
        assert self.peer_url
        url = self.peer_url.rstrip("/") + path
        try:
            with urlopen(url, timeout=5) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise ServiceError(f"联调方 GET {path} 失败：{exc.code} {body}")

    # ------------------------------------------------------------------
    # 查询、归属和客户视图
    # ------------------------------------------------------------------
    def get_switch(self, switch_id: str, *, now: datetime | str | None = None) -> dict[str, Any]:
        if not self.store.get("switch", switch_id):
            raise NotFoundError(f"换签单 {switch_id} 不存在")
        at = parse_dt(now) if now else datetime.now(CHINA_TZ)
        sw = self._refresh_switch(switch_id, now=at)
        return json.loads(json.dumps(sw, ensure_ascii=False))

    def list_switches(
        self,
        *,
        customer: str | None = None,
        retailer: str | None = None,
        meter_id: str | None = None,
        actor: Any = None,
    ) -> list[dict[str, Any]]:
        actor = normalize_actor(actor)
        result = []
        for sw in self.store.list("switch"):
            if customer and sw["customer"] != customer:
                continue
            if meter_id and meter_id not in sw["meter_ids"]:
                continue
            if retailer and retailer not in {sw["out"], sw["incoming"], (sw.get("bridge_terms") or {}).get("owner")}:
                continue
            if actor["type"] == "customer" and not self._actor_allowed_for_customer(sw["customer"], actor):
                continue
            if actor["type"] in {"retailer", "finance"}:
                ident = actor.get("retailer") or actor.get("id")
                if ident not in {sw["out"], sw["incoming"], (sw.get("bridge_terms") or {}).get("owner")}:
                    continue
            result.append(self.get_switch(sw["id"]))
        return result

    def latest_boundary(self, meter_id: str) -> dict[str, Any] | None:
        boundaries = [
            b
            for b in self.store.list("boundary")
            if b["meter_id"] == meter_id and b.get("status") == "active"
        ]
        if not boundaries:
            return None
        return max(boundaries, key=lambda b: parse_dt(b["at"]))

    def list_boundaries(self, meter_id: str | None = None) -> list[dict[str, Any]]:
        rows = self.store.list("boundary")
        if meter_id:
            rows = [b for b in rows if b["meter_id"] == meter_id]
        return sorted(rows, key=lambda b: (b["meter_id"], parse_dt(b["at"])))

    def ownership_at(self, meter_id: str, at: str | datetime) -> dict[str, Any]:
        at_dt = parse_dt(at)
        meter = self.store.get("meter", meter_id)
        owner = meter.get("current") if meter else None
        mode = "base_contract"
        rate_kind = "tariff"
        cents = None
        boundary_id = None
        source = "meter.current"

        boundaries = sorted(
            [b for b in self.list_boundaries(meter_id) if parse_dt(b["at"]) <= at_dt],
            key=lambda b: parse_dt(b["at"]),
        )
        for boundary in boundaries:
            boundary_id = boundary["id"]
            formal_from = boundary.get("formal_from")
            if boundary.get("commercial_mode") == "bridge" and (
                not formal_from or at_dt < parse_dt(formal_from)
            ):
                owner = boundary["commercial_owner"]
                mode = "bridge"
                rate_kind = "bridge"
                cents = boundary.get("cents_per_kwh")
            else:
                owner = boundary.get("formal_commercial_owner") or boundary["commercial_owner"]
                mode = "formal"
                rate_kind = "tariff"
                cents = None
        return {
            "meter_id": meter_id,
            "at": iso(at_dt),
            "owner": owner,
            "retailer": owner,
            "commercial_mode": mode,
            "rate_kind": rate_kind,
            "cents_per_kwh": cents,
            "boundary_id": boundary_id,
            "basis": source if not boundaries else "commercial_segments",
        }

    get_owner = ownership_at

    def resolve_attribution(
        self,
        meter_id: str,
        at: str | datetime,
        *,
        create_if_missing: bool = True,
        exclude_switch_id: str | None = None,
    ) -> dict[str, Any]:
        """查清 at 时点的前一个有效归属。

        若存在计划时间已到、但只有 accepted/尚未收到终局事件的换签，结果为
        undetermined；此时调用方不得对外发出以某个售方为起点的新承诺。
        """
        at_dt = parse_dt(at)
        ownership = self.ownership_at(meter_id, at_dt)
        blocking: list[dict[str, Any]] = []
        for sw in self.store.list("switch"):
            if sw["id"] == exclude_switch_id or meter_id not in sw["meter_ids"]:
                continue
            planned_dt = parse_dt(sw["planned"])
            if planned_dt > at_dt:
                continue
            point = self._reduce_point(sw["id"], meter_id)
            if point["state"] in {ACCEPTED, APPLYING, DECLARED, DRAFT, DELAYED}:
                # 已到/越过计划日但没有 applied/declined/withdrawn 的权威终局。
                blocking.append(
                    {
                        "change_id": sw["id"],
                        "planned": sw["planned"],
                        "point_state": point["state"],
                        "event_id": point.get("event_id"),
                    }
                )
            elif point["state"] == CONFIRMED and not self._boundary_for_change_meter(sw["id"], meter_id):
                blocking.append(
                    {"change_id": sw["id"], "reason": "applied_event_without_processed_boundary"}
                )
        if blocking:
            return {
                "meter_id": meter_id,
                "at": iso(at_dt),
                "status": "undetermined",
                "owner": None,
                "blocking": blocking,
            }
        return {"status": "determined", **ownership}

    previous_valid_owner = resolve_attribution

    def status_board(self, customer: str, actor: Any = None) -> dict[str, Any]:
        """客户视图：明确区分申报中、延期、临时承接和已有确认。"""
        self.require_customer(customer, actor)
        items = []
        for sw in self.list_switches(customer=customer, actor={"type": "system"}):
            points = []
            for mid, state in sw.get("point_states", {}).items():
                points.append(
                    {
                        "meter_id": mid,
                        "state": state.get("state"),
                        "business_state": state.get("business_state") or state.get("state"),
                        "commercial_state": state.get("commercial_state"),
                        "label": STATE_LABELS.get(
                            state.get("business_state") or state.get("state"),
                            state.get("business_state") or state.get("state"),
                        ),
                    }
                )
            items.append(
                {
                    "change_id": sw["id"],
                    "state": sw["state"],
                    "status": sw["status"],
                    "label": sw.get("state_label") or STATE_LABELS.get(sw["state"], sw["state"]),
                    "commercial_state": sw.get("commercial_state"),
                    "planned": sw["planned"],
                    "formal_commercial_at": sw.get("formal_commercial_at"),
                    "commitment_issued": sw.get("commitment_issued"),
                    "attribution_status": sw.get("attribution_status"),
                    "meters": points,
                }
            )
        return {"customer": customer, "items": items}

    def customer_view(self, customer: str, actor: Any = None) -> dict[str, Any]:
        self.require_customer(customer, actor)
        meters = [m for m in self.store.list("meter") if m.get("company") == customer]
        meter_views = []
        for meter in meters:
            mid = meter["id"]
            meter_views.append(
                {
                    "meter": meter,
                    "current_owner": self.ownership_at(mid, now_iso()),
                    "boundaries": self.list_boundaries(mid),
                    "switches": self.list_switches(customer=customer, meter_id=mid),
                    "trace": self.trace_meter(mid, actor={"type": "system"}),
                }
            )
        return {"customer": customer, "meters": meter_views}

    # ------------------------------------------------------------------
    # 读数版本、更正和唯一用量断点
    # ------------------------------------------------------------------
    def ingest_reading(
        self, reading: dict[str, Any], actor: Any = None
    ) -> dict[str, Any]:
        self.require_meter_operator(actor)
        rid = reading.get("id") or _new_id("R")
        if self.store.get("reading", rid):
            return {"id": rid, "idempotent": True, "reading": self.store.get("reading", rid)}
        mid = reading.get("meter") or reading.get("meter_id")
        if not mid:
            raise ValidationError("读数缺少 meter/meter_id")
        if not self.store.get("meter", mid):
            raise NotFoundError(f"计量点 {mid} 不存在")
        try:
            kwh = int(reading["kwh"])
        except (KeyError, TypeError, ValueError):
            raise ValidationError("kwh 必须是整数累计读数")
        if kwh < 0:
            raise ValidationError("累计读数不能为负")
        quality = reading.get("quality", "estimated")
        if quality not in {"confirmed", "estimated", "provisional"}:
            # 不因为未知质量值丢失事实，但视为不可结清。
            quality = "estimated"

        doc = {
            **reading,
            "id": rid,
            "meter": mid,
            "meter_id": mid,
            "time": iso(parse_dt(reading["time"])),
            "kwh": kwh,
            "quality": quality,
            "revision": int(reading.get("revision") or 1),
            "revises": reading.get("revises"),
            "superseded_by": None,
            "received_at": now_iso(),
        }

        old_same_id = self.store.get("reading", rid)
        if old_same_id:
            if old_same_id != doc:
                raise ConflictError(f"读数 {rid} 已存在且内容不同")
            return {"id": rid, "idempotent": True, "reading": old_same_id}

        # 同标识幂等；同时间/revision 的旧报文重放不产生新事实。
        same_version = [
            r
            for r in self.store.list("reading")
            if r["meter"] == mid
            and r["time"] == doc["time"]
            and int(r.get("revision") or 1) == doc["revision"]
        ]
        if same_version:
            return {"id": same_version[0]["id"], "idempotent": True, "reading": same_version[0]}

        self.store.put("reading", rid, doc)

        revised_reading: dict[str, Any] | None = None
        if doc.get("revises"):
            revised_reading = self.store.get("reading", doc["revises"])
            if revised_reading:
                revised_reading["superseded_by"] = rid
                self.store.put("reading", revised_reading["id"], revised_reading)

        same_time = [
            r
            for r in self.store.list("reading")
            if r["meter"] == mid and r["time"] == doc["time"] and r["id"] != rid
        ]
        for old in same_time:
            if int(doc.get("revision") or 1) > int(old.get("revision") or 1):
                if not old.get("superseded_by"):
                    old["superseded_by"] = rid
                    self.store.put("reading", old["id"], old)
                    if revised_reading is None:
                        revised_reading = old
            elif int(doc.get("revision") or 1) < int(old.get("revision") or 1):
                doc["superseded_by"] = old["id"]
                self.store.put("reading", rid, doc)

        # 更正对应的交界断点。
        for boundary in self.store.list("boundary"):
            if boundary["meter_id"] != mid:
                continue
            same_boundary_time = parse_dt(boundary["at"]) == parse_dt(doc["time"])
            references_old = revised_reading and boundary.get("reading_id") == revised_reading["id"]
            references_new = boundary.get("reading_id") == rid
            if same_boundary_time or references_old or references_new:
                current_id = self.current_reading_id(mid, doc["time"])
                current = self.store.get("reading", current_id) if current_id else doc
                boundary["reading_id"] = current["id"]
                boundary["recognized_reading_id"] = current["id"]
                boundary["kwh"] = current["kwh"]
                boundary["reading_quality"] = current.get("quality")
                boundary["revision"] = current.get("revision", 1)
                self.store.put("boundary", boundary["id"], boundary)

        effects: dict[str, Any] = {"draft_bills_updated": [], "adjustments": []}
        if revised_reading and revised_reading["kwh"] != doc["kwh"]:
            effects = self._apply_reading_correction(revised_reading, doc)
        return {"id": rid, "reading": doc, **effects}

    add_reading = ingest_reading
    receive_reading = ingest_reading

    def current_reading_id(self, meter_id: str, time_value: str | datetime) -> str | None:
        at = iso(parse_dt(time_value))
        versions = [
            r
            for r in self.store.list("reading")
            if r["meter"] == meter_id and r["time"] == at and not r.get("superseded_by")
        ]
        if not versions:
            all_versions = [
                r
                for r in self.store.list("reading")
                if r["meter"] == meter_id and r["time"] == at
            ]
            versions = all_versions
        if not versions:
            return None
        current = max(versions, key=lambda r: (int(r.get("revision") or 1), r["id"]))
        return current["id"]

    def canonical_readings(self, meter_id: str) -> list[dict[str, Any]]:
        by_time: dict[str, list[dict[str, Any]]] = {}
        for reading in self.store.list("reading"):
            if reading["meter"] != meter_id:
                continue
            by_time.setdefault(reading["time"], []).append(reading)
        result = []
        for at, versions in by_time.items():
            active = [r for r in versions if not r.get("superseded_by")] or versions
            # 优先 confirmed；同质量取高 revision。
            current = max(
                active,
                key=lambda r: (
                    1 if r.get("quality") == "confirmed" else 0,
                    int(r.get("revision") or 1),
                    r["id"],
                ),
            )
            result.append(current)
        return sorted(result, key=lambda r: parse_dt(r["time"]))

    def boundary_voucher(
        self, boundary_id: str | None = None, *, meter_id: str | None = None, at: str | None = None
    ) -> dict[str, Any]:
        boundary = None
        if boundary_id:
            boundary = self.store.require("boundary", boundary_id)
        elif meter_id and at:
            boundary = self.store.get("boundary", f"BND-{self._change_for_boundary_lookup(meter_id, at)}-{meter_id}")
            if boundary is None:
                matches = [
                    b for b in self.list_boundaries(meter_id) if parse_dt(b["at"]) == parse_dt(at)
                ]
                boundary = matches[-1] if matches else None
        elif meter_id:
            boundary = self.latest_boundary(meter_id)
        if boundary is None:
            raise NotFoundError("没有符合条件的交界凭证")

        mid = boundary["meter_id"]
        current_id = self.current_reading_id(mid, boundary["at"]) or boundary.get("reading_id")
        current = self.store.require("reading", current_id) if current_id else None
        previous = [
            r for r in self.canonical_readings(mid) if parse_dt(r["time"]) < parse_dt(boundary["at"])
        ]
        previous_reading = previous[-1] if previous else None
        history = [
            r
            for r in self.store.list("reading")
            if r["meter"] == mid and r["time"] == (current or {}).get("time", boundary["at"])
        ]
        before_party = boundary.get("before_owner") or boundary.get("old_retailer")
        after_party = boundary.get("commercial_owner") or boundary.get("incoming_retailer")
        return {
            "id": f"V-{boundary['id']}",
            "boundary_id": boundary["id"],
            "meter_id": mid,
            "at": boundary["at"],
            "single_breakpoint": True,
            "recognized_reading": current,
            "recognized_reading_id": current["id"] if current else None,
            "kwh": current["kwh"] if current else boundary.get("kwh"),
            "quality": current.get("quality") if current else boundary.get("reading_quality"),
            "revision": current.get("revision") if current else boundary.get("revision"),
            "previous_reading": previous_reading,
            "usage_to_breakpoint": (
                current["kwh"] - previous_reading["kwh"]
                if current and previous_reading
                else None
            ),
            "closing": {
                "party": before_party,
                "reading_id": current["id"] if current else None,
                "end_kwh": current["kwh"] if current else None,
            },
            "opening": {
                "party": after_party,
                "reading_id": current["id"] if current else None,
                "start_kwh": current["kwh"] if current else None,
            },
            # 期末数和期初数是同一个累计表码；差额只计一次，不形成新增用量。
            "same_kwh_for_closing_and_opening": True,
            "version_history": sorted(history, key=lambda r: int(r.get("revision") or 1)),
            "adjustment_ids": [
                a["id"]
                for a in self.store.list("adjustment")
                if a.get("boundary_id") == boundary["id"]
            ],
            "reconciliation": self.reconciliation_status(boundary["id"]),
        }

    def closing_reading(self, meter_id: str, at: str | None = None) -> dict[str, Any]:
        return self.boundary_voucher(meter_id=meter_id, at=at)["closing"]

    def opening_reading(self, meter_id: str, at: str | None = None) -> dict[str, Any]:
        return self.boundary_voucher(meter_id=meter_id, at=at)["opening"]

    def retailer_settlement_scope(
        self, retailer: str, meter_id: str, *, actor: Any = None
    ) -> dict[str, Any]:
        """旧方期末范围或新方第一期范围；财务身份只能查本售方。"""
        if actor is not None:
            self.require_retailer(retailer, actor)
        calc = self.calculate_settlement(meter_id)
        entries = [e for e in calc["entries"] if e["retailer"] == retailer]
        boundary_ids = {e.get("boundary_id") for e in entries if e.get("boundary_id")}
        vouchers = [
            self.boundary_voucher(bid) for bid in boundary_ids if bid
        ]
        return {
            "retailer": retailer,
            "meter_id": meter_id,
            "entries": entries,
            "total_kwh": sum(e["kwh"] for e in entries if e["settleable"]),
            "total_cents": sum(e["amount_cents"] for e in entries if e["settleable"]),
            "boundary_vouchers": vouchers,
            "holds": calc["holds"],
        }

    def _change_for_boundary_lookup(self, meter_id: str, at: str) -> str | None:
        matches = [b for b in self.list_boundaries(meter_id) if parse_dt(b["at"]) == parse_dt(at)]
        return matches[-1]["change_id"] if matches else None

    # ------------------------------------------------------------------
    # 计量结算、账单签发、更正差额
    # ------------------------------------------------------------------
    def _tariff_for(self, retailer: str, meter: dict[str, Any], at: datetime) -> dict[str, Any] | None:
        candidates = [
            t
            for t in self.store.list("tariff")
            if t.get("retailer") == retailer
            and (not t.get("zones") or meter.get("zone") in t.get("zones", []))
        ]
        # 样例费率没有生效期；若以后提供 effective_from/to，在此按覆盖开始时刻选择。
        for tariff in candidates:
            start = tariff.get("effective_from")
            end = tariff.get("effective_to")
            if start and at < parse_dt(start):
                continue
            if end and at >= parse_dt(end):
                continue
            return tariff
        return candidates[0] if candidates else None

    def calculate_settlement(
        self, meter_id: str, *, start: str | None = None, end: str | None = None
    ) -> dict[str, Any]:
        meter = self.store.require("meter", meter_id)
        reads = self.canonical_readings(meter_id)
        start_dt = parse_dt(start) if start else None
        end_dt = parse_dt(end) if end else None
        entries: list[dict[str, Any]] = []
        holds: list[dict[str, Any]] = []

        for left, right in zip(reads, reads[1:]):
            left_dt = parse_dt(left["time"])
            right_dt = parse_dt(right["time"])
            if start_dt and right_dt <= start_dt:
                continue
            if end_dt and left_dt >= end_dt:
                continue
            if right["kwh"] < left["kwh"]:
                holds.append(
                    {
                        "meter_id": meter_id,
                        "start": left["time"],
                        "end": right["time"],
                        "reason": "cumulative_reading_decreased",
                    }
                )
                continue

            owner_info = self.ownership_at(meter_id, left_dt)
            # 若商业归属在相邻读数之间变化且该时刻没有读数，则不能凭空制造第二个表码断点。
            transition_inside = self._commercial_transition_inside(meter_id, left_dt, right_dt)
            if transition_inside and not self._has_reading_at(meter_id, transition_inside):
                holds.append(
                    {
                        "meter_id": meter_id,
                        "start": left["time"],
                        "end": right["time"],
                        "commercial_transition_at": iso(transition_inside),
                        "reason": "commercial_transition_without_reading; 需补读数或预先约定分摊方法",
                    }
                )
                continue

            kwh = right["kwh"] - left["kwh"]
            owner = owner_info["owner"]
            breakpoint_matches = [
                b for b in self.list_boundaries(meter_id) if parse_dt(b["at"]) == right_dt
            ]
            breakpoint_boundary_id = breakpoint_matches[-1]["id"] if breakpoint_matches else None
            rate_kind = owner_info["rate_kind"]
            tariff_id = None
            if rate_kind == "bridge":
                cents = owner_info["cents_per_kwh"]
            else:
                tariff = self._tariff_for(owner, meter, left_dt) if owner else None
                if tariff is None:
                    holds.append(
                        {
                            "meter_id": meter_id,
                            "start": left["time"],
                            "end": right["time"],
                            "owner": owner,
                            "reason": "tariff_missing",
                        }
                    )
                    continue
                cents = int(tariff["cents_per_kwh"])
                tariff_id = tariff["id"]

            settleable = left.get("quality") == "confirmed" and right.get("quality") == "confirmed"
            entry = {
                "id": f"E-{left['id']}-{right['id']}",
                "meter_id": meter_id,
                "start": left["time"],
                "end": right["time"],
                "start_reading_id": left["id"],
                "end_reading_id": right["id"],
                "start_kwh": left["kwh"],
                "end_kwh": right["kwh"],
                "kwh": kwh,
                "usage_kwh": kwh,
                "usage": kwh,
                "owner": owner,
                "retailer": owner,
                "tariff_id": tariff_id,
                "cents_per_kwh": cents,
                "amount_cents": kwh * cents,
                "amount": kwh * cents,
                "rate_kind": rate_kind,
                "boundary_id": owner_info.get("boundary_id") or breakpoint_boundary_id,
                "breakpoint_boundary_id": breakpoint_boundary_id,
                "quality": "confirmed" if settleable else "estimated",
                "settleable": settleable,
            }
            entries.append(entry)

        totals: dict[str, dict[str, int]] = {}
        for entry in entries:
            if not entry["settleable"]:
                continue
            bucket = totals.setdefault(
                entry["owner"], {"kwh": 0, "amount_cents": 0, "estimated_kwh": 0}
            )
            bucket["kwh"] += entry["kwh"]
            bucket["amount_cents"] += entry["amount_cents"]
        for entry in entries:
            if not entry["settleable"]:
                bucket = totals.setdefault(
                    entry["owner"] or "unknown", {"kwh": 0, "amount_cents": 0, "estimated_kwh": 0}
                )
                bucket["estimated_kwh"] += entry["kwh"]

        return {
            "meter_id": meter_id,
            "customer": meter.get("company"),
            "entries": entries,
            "holds": holds,
            "totals_by_retailer": totals,
        }

    settle = calculate_settlement
    settlement = calculate_settlement

    def _commercial_transition_inside(self, meter_id, left_dt, right_dt):
        for boundary in self.list_boundaries(meter_id):
            formal_from = boundary.get("formal_from")
            if formal_from:
                ts = parse_dt(formal_from)
                if left_dt < ts < right_dt:
                    return ts
            physical = parse_dt(boundary["at"])
            if left_dt < physical < right_dt:
                return physical
        return None

    def _has_reading_at(self, meter_id: str, at: datetime) -> bool:
        return any(parse_dt(r["time"]) == at for r in self.canonical_readings(meter_id))

    def generate_bills(
        self,
        meter_id: str | None = None,
        *,
        customer: str | None = None,
        actor: Any = None,
        bill_id: str | None = None,
    ) -> list[dict[str, Any]]:
        actor = normalize_actor(actor)
        meter_ids = [meter_id] if meter_id else [m["id"] for m in self.store.list("meter")]
        bills: list[dict[str, Any]] = []
        for mid in meter_ids:
            meter = self.store.require("meter", mid)
            if customer and meter.get("company") != customer:
                continue
            if actor["type"] == "customer" and not self._actor_allowed_for_customer(meter["company"], actor):
                raise PermissionDeniedError("无权为该客户生成账单")
            calc = self.calculate_settlement(mid)
            by_owner: dict[str, list[dict[str, Any]]] = {}
            for entry in calc["entries"]:
                by_owner.setdefault(entry["owner"], []).append(entry)
            for owner, lines in by_owner.items():
                deterministic_id = bill_id or f"BILL-{owner}-{mid}"
                existing = self.store.get("bill", deterministic_id)
                if existing and existing["status"] == "issued":
                    bills.append(existing)
                    continue
                settleable_lines = [line for line in lines if line["settleable"]]
                bill = existing or {
                    "id": deterministic_id,
                    "bill_number": deterministic_id,
                    "customer": meter["company"],
                    "retailer": owner,
                    "meter_id": mid,
                    "currency": "CNY",
                    "unit": "cent",
                    "status": "draft",
                    "created_at": now_iso(),
                }
                bill.update(
                    {
                        "lines": settleable_lines,
                        "provisional_lines": [l for l in lines if not l["settleable"]],
                        "holds": calc["holds"],
                        "total_kwh": sum(l["kwh"] for l in settleable_lines),
                        "total_usage_kwh": sum(l["kwh"] for l in settleable_lines),
                        "total_cents": sum(l["amount_cents"] for l in settleable_lines),
                        "total_amount_cents": sum(l["amount_cents"] for l in settleable_lines),
                        "provisional_kwh": sum(l["kwh"] for l in lines if not l["settleable"]),
                        "issuable": not calc["holds"] and all(l["settleable"] for l in lines),
                        "updated_at": now_iso(),
                    }
                )
                self.store.put("bill", bill["id"], bill)
                bills.append(bill)
        return bills

    def issue_bill(self, bill_id: str, actor: Any = None) -> dict[str, Any]:
        bill = self.store.require("bill", bill_id)
        self.require_retailer(bill["retailer"], actor)
        if bill["status"] == "issued":
            return bill
        if not bill.get("lines"):
            raise ValidationError("没有可签发的 confirmed 计量行")
        if bill.get("holds"):
            raise ConflictError("存在未解决计量/费率缺口，不能签发", details={"holds": bill["holds"]})
        if bill.get("provisional_lines"):
            raise ConflictError("估抄行不能结清/签发")
        bill["status"] = "issued"
        bill["issued_at"] = now_iso()
        # 深拷贝冻结签发依据；后续更正只生成 adjustment，不改写已签发账单。
        bill["issued_lines_snapshot"] = json.loads(json.dumps(bill["lines"], ensure_ascii=False))
        self.store.put("bill", bill_id, bill)
        return bill

    def list_bills(self, actor: Any = None, *, retailer: str | None = None, customer: str | None = None):
        actor = normalize_actor(actor)
        rows = self.store.list("bill")
        if actor["type"] in {"retailer", "finance"}:
            ident = actor.get("retailer") or actor.get("id")
            rows = [b for b in rows if b["retailer"] == ident]
        elif actor["type"] == "customer":
            rows = [b for b in rows if self._actor_allowed_for_customer(b["customer"], actor)]
        if retailer:
            self.require_retailer(retailer, actor)
            rows = [b for b in rows if b["retailer"] == retailer]
        if customer:
            self.require_customer(customer, actor)
            rows = [b for b in rows if b["customer"] == customer]
        return rows

    def _apply_reading_correction(
        self, old: dict[str, Any], new: dict[str, Any]
    ) -> dict[str, Any]:
        """已签发账单保留不动，生成/补足可认领调整差额；草稿按新依据重算。"""
        draft_updates: list[str] = []
        adjustment_ids: list[str] = []
        delta_kwh_at_time = new["kwh"] - old["kwh"]

        # 更新所有相关草稿。
        for bill in self.store.list("bill"):
            if bill.get("meter_id") != old["meter"]:
                continue
            if bill["status"] == "draft":
                updated = self.generate_bills(old["meter"], bill_id=bill["id"])
                draft_updates.extend(b["id"] for b in updated)

        # 找到签发时覆盖该表码时点的行。支持 R-B1 -> R-B2 -> R-B3 的累计更正。
        for bill in self.store.list("bill"):
            if bill.get("meter_id") != old["meter"] or bill["status"] != "issued":
                continue
            snapshot = bill.get("issued_lines_snapshot") or bill.get("lines") or []
            for line in snapshot:
                role = None
                if line.get("end") == old["time"] or line.get("end_reading_id") == old["id"]:
                    role = "end"
                elif line.get("start") == old["time"] or line.get("start_reading_id") == old["id"]:
                    role = "start"
                if not role:
                    continue

                original_kwh = line["kwh"]
                if role == "end":
                    current_usage = new["kwh"] - line["start_kwh"]
                else:
                    current_usage = line["end_kwh"] - new["kwh"]
                total_delta_kwh = current_usage - original_kwh
                total_delta_cents = total_delta_kwh * int(line["cents_per_kwh"])

                prior = [
                    a
                    for a in self.store.list("adjustment")
                    if a.get("bill_id") == bill["id"] and a.get("bill_line_id") == line["id"]
                ]
                already_kwh = sum(int(a["delta_kwh"]) for a in prior)
                already_cents = sum(int(a["delta_cents"]) for a in prior)
                remaining_kwh = total_delta_kwh - already_kwh
                remaining_cents = total_delta_cents - already_cents
                if remaining_kwh == 0 and remaining_cents == 0:
                    continue

                boundary = None
                boundaries = [
                    b
                    for b in self.list_boundaries(old["meter"])
                    if parse_dt(b["at"]) == parse_dt(old["time"])
                ]
                if boundaries:
                    boundary = boundaries[-1]
                adj_id = f"ADJ-{new['id']}-{bill['id']}-{line['id']}-{role}"
                if self.store.get("adjustment", adj_id):
                    continue
                adjustment = {
                    "id": adj_id,
                    "status": "claimable",
                    "bill_id": bill["id"],
                    "bill_number": bill.get("bill_number"),
                    "bill_line_id": line["id"],
                    "retailer": bill["retailer"],
                    "customer": bill["customer"],
                    "meter_id": old["meter"],
                    "boundary_id": boundary["id"] if boundary else line.get("boundary_id"),
                    "correction_reading_id": new["id"],
                    "old_reading_id": old["id"],
                    "role": role,
                    "delta_kwh": remaining_kwh,
                    "usage_delta_kwh": remaining_kwh,
                    "delta_cents": remaining_cents,
                    "amount_delta_cents": remaining_cents,
                    "amount_delta": remaining_cents,
                    "currency": "CNY",
                    "unit": "cent",
                    "reason": "meter_reading_correction",
                    "claimed": False,
                    "claimed_by": None,
                    "claimed_at": None,
                    "acknowledged": False,
                    "acknowledged_by": None,
                    "acknowledged_at": None,
                    "created_at": now_iso(),
                }
                self.store.put("adjustment", adj_id, adjustment)
                adjustment_ids.append(adj_id)
        return {"draft_bills_updated": sorted(set(draft_updates)), "adjustment_ids": adjustment_ids}

    def claim_adjustment(self, adjustment_id: str, actor: Any = None, *, acknowledge: bool = False):
        adj = self.store.require("adjustment", adjustment_id)
        actor = self.require_retailer(adj["retailer"], actor)
        if not adj.get("claimed"):
            adj["claimed"] = True
            adj["claimed_by"] = normalize_actor(actor).get("id")
            adj["claimed_at"] = now_iso()
            adj["status"] = "claimed"
        if acknowledge:
            adj["acknowledged"] = True
            adj["acknowledged_by"] = normalize_actor(actor).get("id")
            adj["acknowledged_at"] = now_iso()
            adj["status"] = "acknowledged"
        self.store.put("adjustment", adjustment_id, adj)
        if adj.get("boundary_id"):
            self.reconciliation_status(adj["boundary_id"])
        return adj

    def acknowledge_adjustment(self, adjustment_id: str, actor: Any = None):
        return self.claim_adjustment(adjustment_id, actor, acknowledge=True)

    def list_adjustments(self, actor: Any = None, *, retailer: str | None = None):
        actor = normalize_actor(actor)
        rows = self.store.list("adjustment")
        if actor["type"] in {"retailer", "finance"}:
            ident = actor.get("retailer") or actor.get("id")
            rows = [a for a in rows if a["retailer"] == ident]
        if retailer:
            self.require_retailer(retailer, actor)
            rows = [a for a in rows if a["retailer"] == retailer]
        return rows

    def provide_party_reading(
        self, boundary_id: str, party: str, reading: dict[str, Any] | str | int, actor: Any = None
    ) -> dict[str, Any]:
        boundary = self.store.require("boundary", boundary_id)
        if actor is not None:
            self.require_retailer(party, actor)
        if isinstance(reading, str) and self.store.get("reading", reading):
            reading_id = reading
            kwh = self.store.get("reading", reading_id)["kwh"]
        elif isinstance(reading, dict):
            reading_id = reading.get("id")
            kwh = int(reading["kwh"])
        else:
            reading_id = None
            kwh = int(reading)
        record = {
            "boundary_id": boundary_id,
            "party": party,
            "reading_id": reading_id,
            "kwh": kwh,
            "received_at": now_iso(),
        }
        self.store.put("party_reading", f"{boundary_id}:{party}", record)
        return self.reconciliation_status(boundary_id)

    def reconciliation_status(self, boundary_id: str) -> dict[str, Any]:
        boundary = self.store.get("boundary", boundary_id)
        if not boundary:
            return {"boundary_id": boundary_id, "status": "missing"}
        canonical_id = self.current_reading_id(boundary["meter_id"], boundary["at"])
        canonical = self.store.get("reading", canonical_id) if canonical_id else None
        party_readings = [
            p for p in self.store.list("party_reading") if p["boundary_id"] == boundary_id
        ]
        adjustments = [
            a for a in self.store.list("adjustment") if a.get("boundary_id") == boundary_id
        ]
        parties = {boundary.get("before_owner"), boundary.get("commercial_owner")}
        parties.discard(None)
        party_status = {}
        for party in parties:
            reported = next((p for p in party_readings if p["party"] == party), None)
            party_adjustments = [a for a in adjustments if a["retailer"] == party]
            acknowledged = all(a.get("acknowledged") for a in party_adjustments) if party_adjustments else False
            matches = bool(canonical and reported and reported["kwh"] == canonical["kwh"])
            has_basis = matches or acknowledged or (not party_adjustments and canonical is not None and reported is not None and matches)
            party_status[party] = {
                "reported": reported,
                "matches_canonical": matches,
                "adjustment_ids": [a["id"] for a in party_adjustments],
                "adjustments_acknowledged": acknowledged,
                "has_basis": has_basis,
            }
        both_have_basis = bool(parties) and all(v["has_basis"] for v in party_status.values())
        unresolved_adjustments = [a["id"] for a in adjustments if not a.get("acknowledged")]
        status = "balanced" if both_have_basis and not unresolved_adjustments else "open"
        result = {
            "boundary_id": boundary_id,
            "status": status,
            "canonical_reading_id": canonical_id,
            "canonical_kwh": canonical["kwh"] if canonical else None,
            "parties": party_status,
            "adjustment_ids": [a["id"] for a in adjustments],
            "unresolved_adjustment_ids": unresolved_adjustments,
            "note": "更正只调整差额，不把计量点重新切换回旧方",
        }
        recon = self.store.get("reconciliation", boundary_id) or {"boundary_id": boundary_id}
        recon.update(result)
        self.store.put("reconciliation", boundary_id, recon)
        return result

    # ------------------------------------------------------------------
    # 财务隔离与企业全链路追踪
    # ------------------------------------------------------------------
    def finance_view(self, actor: Any = None, retailer: str | None = None) -> dict[str, Any]:
        actor = normalize_actor(actor)
        if actor["type"] not in {"system", "admin"}:
            retailer = retailer or actor.get("retailer") or actor.get("id")
            if actor["type"] not in {"retailer", "finance"} or not retailer:
                raise PermissionDeniedError("财务视图需要售方/财务身份")
            self.require_retailer(retailer, actor)
        elif not retailer:
            raise ValidationError("系统身份查询财务视图时必须指定 retailer")

        bills = self.list_bills(actor={"type": "system"}, retailer=retailer)
        adjustments = self.list_adjustments(actor={"type": "system"}, retailer=retailer)
        meter_ids = {b["meter_id"] for b in bills} | {a["meter_id"] for a in adjustments}
        scoped_entries = []
        for meter in self.store.list("meter"):
            calc = self.calculate_settlement(meter["id"])
            own_entries = [e for e in calc["entries"] if e.get("retailer") == retailer]
            if own_entries:
                meter_ids.add(meter["id"])
                scoped_entries.extend(own_entries)
        meters = [self.store.get("meter", mid) for mid in sorted(meter_ids)]
        tariffs = [t for t in self.store.list("tariff") if t.get("retailer") == retailer]
        # 只返回该售方账单行/结算行涉及的计量读数和自己的费率，不返回其他售方明细。
        reading_ids: set[str] = set()
        for line in scoped_entries:
            reading_ids.add(line.get("start_reading_id"))
            reading_ids.add(line.get("end_reading_id"))
        for bill in bills:
            for line in bill.get("lines", []) + bill.get("issued_lines_snapshot", []):
                reading_ids.add(line.get("start_reading_id"))
                reading_ids.add(line.get("end_reading_id"))
        readings = [self.store.get("reading", rid) for rid in reading_ids if rid and self.store.get("reading", rid)]
        return {
            "retailer": retailer,
            "bills": bills,
            "adjustments": adjustments,
            "meters": meters,
            "tariffs": tariffs,
            "meter_readings": readings,
        }

    def trace_meter(self, meter_id: str, actor: Any = None) -> dict[str, Any]:
        meter = self.store.require("meter", meter_id)
        if actor is not None and normalize_actor(actor)["type"] not in {"system", "admin", "meter"}:
            self.require_customer(meter["company"], actor)
        switches = [s for s in self.store.list("switch") if meter_id in s["meter_ids"]]
        boundaries = self.list_boundaries(meter_id)
        boundary_ids = {b["id"] for b in boundaries}
        bills = [b for b in self.store.list("bill") if b.get("meter_id") == meter_id]
        adjustments = [a for a in self.store.list("adjustment") if a.get("meter_id") == meter_id]
        readings = self.store.list("reading")
        readings = [r for r in readings if r["meter"] == meter_id]
        links = []
        for boundary in boundaries:
            links.append(
                {
                    "type": "boundary",
                    "id": boundary["id"],
                    "change_id": boundary["change_id"],
                    "reading_id": boundary.get("recognized_reading_id"),
                    "bill_ids": [b["id"] for b in bills if any(l.get("boundary_id") == boundary["id"] for l in b.get("lines", []))],
                    "adjustment_ids": [a["id"] for a in adjustments if a.get("boundary_id") == boundary["id"]],
                    "voucher_id": f"V-{boundary['id']}",
                }
            )
        return {
            "meter_id": meter_id,
            "customer": meter.get("company"),
            "meter": meter,
            "ownership_now": self.ownership_at(meter_id, now_iso()),
            "switches": [self.get_switch(s["id"]) for s in switches],
            "boundaries": boundaries,
            "readings": sorted(readings, key=lambda r: parse_dt(r["time"])),
            "bills": bills,
            "adjustments": adjustments,
            "links": links,
        }

    # 常见调用命名的语义别名。
    cancel_switch = request_withdrawal
    withdraw = request_withdrawal
    receive_meter_result = ingest_events
    receive_switch_result = ingest_events
    report_events = ingest_events
    report_reading = ingest_reading
    add_meter_reading = ingest_reading
    apply_reading_correction = ingest_reading
    load_data = seed_directory
    load_fixtures = seed_directory
    seed = seed_directory
    get_settlement = calculate_settlement
    settle_meter = calculate_settlement
    get_ownership = ownership_at
    get_attribution = resolve_attribution
    agree_switch = agree

    def postpone_switch(
        self,
        switch_id: str,
        new_planned: str | datetime,
        *,
        reason: str | None = None,
        actor: Any = None,
    ) -> dict[str, Any]:
        """记录延期/改期；交界未成立前责任仍归原方。"""
        sw = self.store.require("switch", switch_id)
        self.require_customer(sw["customer"], actor)
        new_at = iso(parse_dt(new_planned))
        sw.setdefault("schedule_history", []).append(
            {"from": sw["planned"], "to": new_at, "reason": reason, "at": now_iso()}
        )
        sw["planned"] = new_at
        sw["planned_at"] = new_at
        sw["delay_reason"] = reason
        sw["state"] = DELAYED
        sw["status"] = DELAYED
        self.store.put("switch", switch_id, sw)
        return self.get_switch(switch_id)

    delay_switch = postpone_switch

    def schedule_switch(
        self,
        switch_id: str,
        planned_at: str | datetime | None = None,
        *,
        actor: Any = None,
        agreed_at: str | datetime | None = None,
        party: str | None = None,
    ) -> dict[str, Any]:
        """记录/调整双方约定的计划切换日，并收集约定确认。"""
        sw = self.store.require("switch", switch_id)
        if planned_at is not None:
            sw["planned"] = iso(parse_dt(planned_at))
            sw["planned_at"] = sw["planned"]
            self.store.put("switch", switch_id, sw)
        return self.agree(switch_id, actor, party=party, at=agreed_at)


class PeerDroppedConnection(Exception):
    """联调方在首次匹配 POST 后直接关闭连接。"""


# ----------------------------------------------------------------------
# 无套接字 JSON 路由：HTTP Handler 与测试可共用 dispatch。
# ----------------------------------------------------------------------
def actor_from_headers(headers: dict[str, str] | Any) -> dict[str, Any]:
    def get(name: str) -> str | None:
        if isinstance(headers, dict):
            return headers.get(name) or headers.get(name.lower())
        return headers.get(name)

    actor_type = get("X-Actor-Type") or get("x-actor-type") or "anonymous"
    actor_id = get("X-Actor-Id") or get("x-actor-id") or actor_type
    actor: dict[str, Any] = {"type": actor_type, "id": actor_id}
    for header, key in (
        ("X-Actor-Customer", "customer"),
        ("X-Actor-Retailer", "retailer"),
    ):
        value = get(header) or get(header.lower())
        if value:
            actor[key] = value
    if actor_type in {"customer", "retailer", "finance"}:
        actor.setdefault(key_for_type(actor_type), actor_id)
    return actor


def key_for_type(actor_type: str) -> str:
    return "customer" if actor_type == "customer" else "retailer"


class Api:
    def __init__(self, service: SwitchService):
        self.service = service

    def dispatch(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        headers: dict[str, str] | Any | None = None,
    ) -> tuple[int, dict[str, Any]]:
        payload = payload or {}
        headers = headers or {}
        if "?" in path:
            from urllib.parse import parse_qs, urlparse

            query = parse_qs(urlparse(path).query)
            for key, values in query.items():
                value = values[-1] if len(values) == 1 else values
                payload.setdefault(key, value)
        actor = actor_from_headers(headers)
        # JSON body 可覆盖/补充 actor，便于脚本和测试。
        if isinstance(payload, dict) and payload.get("actor"):
            actor = normalize_actor(payload["actor"])
        try:
            return self._dispatch(method.upper(), path, payload, actor)
        except ServiceError as exc:
            return exc.status, {"error": exc.code, "message": exc.message, "details": exc.details}
        except (ValueError, TypeError, KeyError) as exc:
            return 400, {"error": "bad_request", "message": str(exc)}

    def _dispatch(self, method: str, path: str, payload: dict, actor: dict) -> tuple[int, dict]:
        s = self.service
        path_only = path.split("?", 1)[0].rstrip("/") or "/"
        parts = [p for p in path_only.split("/") if p]

        if method == "GET" and path_only in {"/", "/health"}:
            return 200, {"ok": True, "service": "electricity-switch-service", "time": now_iso()}

        if path_only.startswith("/api/") and actor.get("type") == "anonymous":
            return 401, {
                "error": "authentication_required",
                "message": "请通过 X-Actor-Type/X-Actor-Id 标明 customer、retailer、finance 或 meter 身份；本机管理调用可用 system",
            }

        if path_only == "/api/meters" and method == "GET":
            meters = s.store.list("meter")
            if actor["type"] == "customer":
                customer = actor.get("customer") or actor.get("id")
                meters = [m for m in meters if m.get("company") == customer]
            elif actor["type"] in {"retailer", "finance"}:
                view = s.finance_view(actor)
                allowed = {m["id"] for m in view["meters"]}
                meters = [m for m in meters if m["id"] in allowed]
            return 200, {"items": meters}

        if path_only == "/api/agreements" and method == "POST":
            return 201, s.register_master_agreement(payload, actor)

        if path_only == "/api/switches":
            if method == "POST":
                return 201, s.create_switch(payload, actor, idempotency_key=payload.get("idempotency_key"))
            if method == "GET":
                return 200, {"items": s.list_switches(actor=actor, customer=payload.get("customer"), retailer=payload.get("retailer"), meter_id=payload.get("meter_id"))}

        if len(parts) >= 3 and parts[0] == "api" and parts[1] == "switches":
            sid = parts[2]
            if len(parts) == 3 and method == "GET":
                return 200, s.get_switch(sid, now=payload.get("now"))
            if len(parts) == 4:
                action = parts[3]
                if action == "agree" and method == "POST":
                    return 200, s.agree(sid, actor, party=payload.get("party"), at=payload.get("at"))
                if action in {"delay", "postpone"} and method == "POST":
                    return 200, s.postpone_switch(sid, payload.get("planned") or payload["new_planned"], reason=payload.get("reason"), actor=actor)
                if action == "commitment" and method == "POST":
                    return 200, s.issue_commitment(sid, actor)
                if action in {"submit", "apply"} and method == "POST":
                    return 200, s.submit_to_meter(sid, actor=actor, call_peer=bool(payload.get("call_peer", bool(s.peer_url))), poll=bool(payload.get("poll")))
                if action == "poll" and method == "POST":
                    return 200, s.poll_meter(sid, actor=actor)
                if action == "withdraw" and method == "POST":
                    return 200, s.request_withdrawal(sid, payload.get("meter_ids"), actor=actor, reason=payload.get("reason"), call_peer=bool(payload.get("call_peer", bool(s.peer_url))))
                if action == "decision" and method == "POST":
                    result = s.decide_remaining(
                        sid,
                        payload["decision"],
                        meter_ids=payload.get("meter_ids"),
                        note=payload.get("note"),
                        create_followup=bool(payload.get("create_followup")),
                        planned=payload.get("planned"),
                        incoming=payload.get("incoming"),
                        actor=actor,
                        idempotency_key=payload.get("idempotency_key"),
                    )
                    return 200, result

        if path_only == "/api/events" and method == "POST":
            return 200, s.ingest_events(payload, actor, context=payload.get("context", "switch"), source_path=payload.get("source_path"))

        if path_only == "/api/readings":
            if method == "POST":
                return 201, s.ingest_reading(payload, actor)
            if method == "GET":
                mid = payload.get("meter_id")
                if actor["type"] in {"retailer", "finance"}:
                    raise PermissionDeniedError("售方财务计量明细请使用 /api/finance 或 /api/settlement-scope")
                if mid and actor["type"] == "customer":
                    meter = s.store.require("meter", mid)
                    s.require_customer(meter["company"], actor)
                return 200, {"items": s.canonical_readings(mid) if mid else s.store.list("reading")}

        if path_only == "/api/ownership" and method == "GET":
            return 200, s.ownership_at(payload["meter_id"], payload["at"])
        if path_only == "/api/attribution" and method == "GET":
            return 200, s.resolve_attribution(payload["meter_id"], payload["at"])

        if path_only == "/api/settlement" and method in {"GET", "POST"}:
            mid = payload["meter_id"]
            if actor["type"] in {"retailer", "finance"}:
                retailer = actor.get("retailer") or actor.get("id")
                return 200, s.retailer_settlement_scope(retailer, mid, actor=actor)
            if actor["type"] == "customer":
                meter = s.store.require("meter", mid)
                s.require_customer(meter["company"], actor)
            return 200, s.calculate_settlement(mid, start=payload.get("start"), end=payload.get("end"))
        if path_only == "/api/settlement-scope" and method in {"GET", "POST"}:
            return 200, s.retailer_settlement_scope(payload["retailer"], payload["meter_id"], actor=actor)

        if path_only == "/api/boundaries" and method == "GET":
            mid = payload.get("meter_id")
            if mid and actor["type"] == "customer":
                meter = s.store.require("meter", mid)
                s.require_customer(meter["company"], actor)
            items = s.list_boundaries(mid)
            if actor["type"] in {"retailer", "finance"}:
                retailer = actor.get("retailer") or actor.get("id")
                items = [
                    b for b in items
                    if retailer in {b.get("before_owner"), b.get("commercial_owner"), b.get("formal_commercial_owner")}
                ]
                for b in items:
                    if b.get("commercial_owner") != retailer:
                        b.pop("cents_per_kwh", None)
            return 200, {"items": items}
        if path_only == "/api/voucher" and method == "GET":
            voucher = s.boundary_voucher(payload.get("boundary_id"), meter_id=payload.get("meter_id"), at=payload.get("at"))
            if actor["type"] == "customer":
                meter = s.store.require("meter", voucher["meter_id"])
                s.require_customer(meter["company"], actor)
            elif actor["type"] in {"retailer", "finance"}:
                retailer = actor.get("retailer") or actor.get("id")
                parties = {voucher["closing"]["party"], voucher["opening"]["party"]}
                if retailer not in parties:
                    raise PermissionDeniedError("该售方不是此交界的对账双方")
            return 200, voucher

        if path_only == "/api/bills/generate" and method == "POST":
            return 201, {"items": s.generate_bills(payload.get("meter_id"), customer=payload.get("customer"), actor=actor, bill_id=payload.get("bill_id"))}
        if path_only == "/api/bills" and method == "GET":
            return 200, {"items": s.list_bills(actor, retailer=payload.get("retailer"), customer=payload.get("customer"))}
        if len(parts) == 4 and parts[:2] == ["api", "bills"] and parts[3] == "issue" and method == "POST":
            return 200, s.issue_bill(parts[2], actor)
        if len(parts) == 3 and parts[:2] == ["api", "bills"] and method == "GET":
            return 200, s.store.require("bill", parts[2])

        if path_only == "/api/adjustments" and method == "GET":
            return 200, {"items": s.list_adjustments(actor, retailer=payload.get("retailer"))}
        if len(parts) == 4 and parts[:2] == ["api", "adjustments"] and parts[3] in {"claim", "acknowledge"} and method == "POST":
            return 200, s.claim_adjustment(parts[2], actor, acknowledge=parts[3] == "acknowledge")

        if path_only == "/api/party-readings" and method == "POST":
            return 200, s.provide_party_reading(payload["boundary_id"], payload["party"], payload.get("reading") or payload.get("kwh") or payload.get("reading_id"), actor)
        if path_only == "/api/reconciliation" and method == "GET":
            return 200, s.reconciliation_status(payload["boundary_id"])

        if path_only == "/api/finance" and method == "GET":
            return 200, s.finance_view(actor, retailer=payload.get("retailer"))
        if len(parts) == 3 and parts[:2] == ["api", "trace"] and method == "GET":
            return 200, s.trace_meter(parts[2], actor)
        if path_only == "/api/status-board" and method == "GET":
            return 200, s.status_board(payload["customer"], actor)
        if path_only == "/api/customer-view" and method == "GET":
            return 200, s.customer_view(payload["customer"], actor)

        return 404, {"error": "not_found", "message": f"没有路由 {method} {path_only}"}


def build_http_server(service: SwitchService, host: str = "127.0.0.1", port: int = 8410) -> ThreadingHTTPServer:
    api = Api(service)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def _send(self, status: int, body: dict[str, Any]) -> None:
            data = json.dumps(body, ensure_ascii=False, indent=2).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _handle(self, method: str) -> None:
            length = int(self.headers.get("Content-Length", "0") or 0)
            payload: dict[str, Any] = {}
            if length:
                try:
                    raw = self.rfile.read(length)
                    payload = json.loads(raw.decode("utf-8"))
                except (UnicodeError, json.JSONDecodeError):
                    self._send(400, {"error": "invalid_json"})
                    return
            # GET 查询参数也并入 payload，方便直接用浏览器/curl。
            if "?" in self.path:
                from urllib.parse import parse_qs, urlparse

                query = parse_qs(urlparse(self.path).query)
                for key, values in query.items():
                    payload.setdefault(key, values[-1] if len(values) == 1 else values)
            status, body = api.dispatch(method, self.path, payload, self.headers)
            self._send(status, body)

        def do_GET(self) -> None:
            self._handle("GET")

        def do_POST(self) -> None:
            self._handle("POST")

        def log_message(self, fmt: str, *args: Any) -> None:
            # 测试输出保持安静；需要访问日志时可改为 stderr。
            return

    return ThreadingHTTPServer((host, port), Handler)


_default_service: SwitchService | None = None
Service = SwitchService
Ledger = SwitchService


def get_default_service() -> SwitchService:
    """模块级便捷服务；默认装入 data/，测试也可设置 SWITCH_DB。"""
    global _default_service
    if _default_service is None:
        db_path = os.environ.get("SWITCH_DB", ":memory:")
        data_dir = os.environ.get("SWITCH_DATA_DIR", str(ROOT / "data"))
        _default_service = SwitchService(db_path, data_dir=data_dir)
    return _default_service


def create_switch(*args: Any, **kwargs: Any):
    return get_default_service().create_switch(*args, **kwargs)


def ingest_events(*args: Any, **kwargs: Any):
    return get_default_service().ingest_events(*args, **kwargs)


def ingest_reading(*args: Any, **kwargs: Any):
    return get_default_service().ingest_reading(*args, **kwargs)


def resolve_attribution(*args: Any, **kwargs: Any):
    return get_default_service().resolve_attribution(*args, **kwargs)


def ownership_at(*args: Any, **kwargs: Any):
    return get_default_service().ownership_at(*args, **kwargs)


def calculate_settlement(*args: Any, **kwargs: Any):
    return get_default_service().calculate_settlement(*args, **kwargs)


def boundary_voucher(*args: Any, **kwargs: Any):
    return get_default_service().boundary_voucher(*args, **kwargs)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="售电切换与计量断点服务")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8410)
    parser.add_argument("--db", default="switch_service.sqlite3")
    parser.add_argument("--data", default=str(ROOT / "data"))
    parser.add_argument("--peer-url", default="http://127.0.0.1:8412")
    parser.add_argument("--no-peer", action="store_true")
    args = parser.parse_args(argv)

    service = SwitchService(
        args.db,
        data_dir=args.data,
        peer_url=None if args.no_peer else args.peer_url,
    )
    server = build_http_server(service, args.host, args.port)
    print(f"售电切换服务: http://{args.host}:{server.server_port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        service.store.close()


if __name__ == "__main__":
    main()
