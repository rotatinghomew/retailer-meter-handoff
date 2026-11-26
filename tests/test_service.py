import json
import sys
import tempfile
import unittest
from pathlib import Path
from threading import Thread

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from service import (  # noqa: E402
    Api,
    AttributionUndeterminedError,
    ConflictError,
    PermissionDeniedError,
    SwitchService,
)

DATA = ROOT / "data"


def load_peer_script():
    return json.loads((DATA / "peer.json").read_text(encoding="utf-8"))


def feed_ch14_events(service):
    script = load_peer_script()
    apply_responses = script[0]["responses"]
    service.ingest_events(apply_responses[0], actor={"type": "meter", "id": "test"})
    service.ingest_events(apply_responses[1], actor={"type": "meter", "id": "test"})
    # 第三条是旧 accepted 重放。
    service.ingest_events(apply_responses[2], actor={"type": "meter", "id": "test"})
    service.ingest_events(
        script[1]["responses"][0],
        actor={"type": "meter", "id": "test"},
        context="withdrawal",
    )


class SeededScenarioTest(unittest.TestCase):
    def setUp(self):
        self.service = SwitchService(":memory:", data_dir=DATA)

    def test_partial_both_group_keeps_physical_boundary_and_old_remaining_point(self):
        feed_ch14_events(self.service)
        ch14 = self.service.get_switch("CH-14")

        self.assertEqual(ch14["state"], "partial_needs_decision")
        self.assertEqual(ch14["point_states"]["E-1"]["state"], "confirmed")
        # E-2 先收到 declined，之后撤回回执为 withdrawn；业务上均未形成交界。
        self.assertEqual(ch14["point_states"]["E-2"]["state"], "declined")
        self.assertEqual(ch14["applied_meter_ids"], ["E-1"])
        self.assertEqual(ch14["failed_meter_ids"], ["E-2"])

        e1_boundary = self.service.latest_boundary("E-1")
        self.assertEqual(e1_boundary["at"], "2027-02-02T00:00:00+08:00")
        self.assertEqual(e1_boundary["recognized_reading_id"], "R-B2")
        self.assertEqual(e1_boundary["commercial_mode"], "bridge")
        self.assertEqual(e1_boundary["commercial_owner"], "松江零售")
        self.assertEqual(e1_boundary["cents_per_kwh"], 55)

        # E-2 没有交界，仍归原方，不能因为 CH-14 申报或撤回事件而两边计价。
        owner = self.service.ownership_at("E-2", "2027-02-02T12:00:00+08:00")
        self.assertEqual(owner["owner"], "松江零售")
        self.assertIsNone(self.service.latest_boundary("E-2"))

        decided = self.service.decide_remaining(
            "CH-14", "stop", actor="customer:桃源板材"
        )
        self.assertEqual(decided["switch"]["state"], "bridging")

    def test_ch15_becomes_commitment_ready_after_prior_is_resolved(self):
        before = self.service.get_switch("CH-15")
        self.assertEqual(before["attribution_status"], "undetermined")
        self.assertFalse(before["commitment_issued"])

        feed_ch14_events(self.service)
        ch15 = self.service.get_switch("CH-15")
        self.assertEqual(ch15["attribution_status"], "determined")
        self.assertTrue(ch15["commitment_allowed"])
        self.assertTrue(ch15["commitment_issued"])
        attribution = self.service.resolve_attribution(
            "E-2", ch15["planned"], exclude_switch_id="CH-15"
        )
        self.assertEqual(attribution["status"], "determined")
        self.assertEqual(attribution["owner"], "松江零售")

    def test_single_corrected_breakpoint_settlement(self):
        feed_ch14_events(self.service)
        settlement = self.service.calculate_settlement("E-1")
        by_start = {line["start"]: line for line in settlement["entries"]}

        before = by_start["2027-02-01T00:00:00+08:00"]
        bridge = by_start["2027-02-02T00:00:00+08:00"]

        # R-B1 13150 被 R-B2 13163 更正；交界前为 163，交界后为 117。
        self.assertEqual((before["kwh"], before["amount_cents"]), (163, 8150))
        self.assertEqual(before["retailer"], "松江零售")
        self.assertEqual(before["cents_per_kwh"], 50)
        self.assertEqual((bridge["kwh"], bridge["amount_cents"]), (117, 6435))
        self.assertEqual(bridge["rate_kind"], "bridge")
        self.assertEqual(bridge["cents_per_kwh"], 55)

        e2 = self.service.calculate_settlement("E-2")
        self.assertEqual(e2["entries"][0]["kwh"], 320)
        self.assertEqual(e2["entries"][0]["amount_cents"], 16000)

        voucher = self.service.boundary_voucher(meter_id="E-1")
        self.assertTrue(voucher["same_kwh_for_closing_and_opening"])
        self.assertEqual(voucher["kwh"], 13163)
        self.assertEqual(voucher["closing"]["end_kwh"], 13163)
        self.assertEqual(voucher["opening"]["start_kwh"], 13163)
        self.assertEqual(len(voucher["version_history"]), 2)

    def test_duplicate_event_replay_does_not_duplicate_fact(self):
        script = load_peer_script()
        first = self.service.ingest_events(
            script[0]["responses"][1], actor={"type": "meter"}
        )
        second = self.service.ingest_events(
            script[0]["responses"][1], actor={"type": "meter"}
        )
        self.assertEqual(len(first["processed"]), 2)
        self.assertEqual(second["processed"], [])
        self.assertEqual(len(second["duplicates"]), 2)
        boundaries = [b for b in self.service.list_boundaries("E-1") if b["change_id"] == "CH-14"]
        self.assertEqual(len(boundaries), 1)


class AttributionAndNewApplicationTest(unittest.TestCase):
    def make_service(self):
        service = SwitchService(":memory:")
        service.upsert_meter({"id": "M1", "company": "C", "current": "OLD", "zone": "Z"})
        service.store.put(
            "tariff", "T-OLD",
            {"id": "T-OLD", "retailer": "OLD", "zones": ["Z"], "cents_per_kwh": 50},
        )
        service.store.put(
            "tariff", "T-NEW",
            {"id": "T-NEW", "retailer": "NEW", "zones": ["Z"], "cents_per_kwh": 63},
        )
        service.store.put(
            "tariff", "T-NEXT",
            {"id": "T-NEXT", "retailer": "NEXT", "zones": ["Z"], "cents_per_kwh": 62},
        )
        return service

    def test_unresolved_previous_application_blocks_commitment_but_not_record(self):
        service = self.make_service()
        service.create_switch(
            {
                "id": "ONE",
                "company": "C",
                "out": "OLD",
                "incoming": "NEW",
                "group": "separate",
                "meter_ids": ["M1"],
                "planned": "2027-03-01T00:00:00+08:00",
                "agreed": True,
            }
        )
        service.ingest_events(
            {"events": [{
                "change_id": "ONE", "meter_id": "M1", "status": "accepted",
                "at": "2027-02-28T10:00:00+08:00", "revision": 1,
            }]},
            actor={"type": "meter"},
        )
        unresolved = service.resolve_attribution("M1", "2027-03-02T00:00:00+08:00")
        self.assertEqual(unresolved["status"], "undetermined")

        new_request = service.create_switch(
            {
                "id": "TWO",
                "company": "C",
                "out": "OLD",
                "incoming": "NEXT",
                "group": "separate",
                "meter_ids": ["M1"],
                "planned": "2027-03-02T00:00:00+08:00",
            }
        )
        self.assertEqual(new_request["attribution_status"], "undetermined")
        self.assertFalse(new_request["commitment_issued"])
        with self.assertRaises(AttributionUndeterminedError):
            service.issue_commitment("TWO", actor="customer:C")

    def test_new_contract_cannot_rewrite_confirmed_boundary(self):
        service = self.make_service()
        service.create_switch(
            {
                "id": "ONE", "company": "C", "out": "OLD", "incoming": "NEW",
                "group": "separate", "meter_ids": ["M1"],
                "planned": "2027-03-01T00:00:00+08:00", "agreed": True,
            }
        )
        service.ingest_events(
            {"events": [{
                "change_id": "ONE", "meter_id": "M1", "status": "applied",
                "at": "2027-03-01T00:00:00+08:00", "revision": 2,
                "reading_id": "B1",
            }]},
            actor={"type": "meter"},
        )
        service.ingest_reading(
            {"id": "B1", "meter": "M1", "time": "2027-03-01T00:00:00+08:00",
             "kwh": 200, "quality": "confirmed"},
            actor={"type": "meter"},
        )

        with self.assertRaises(ConflictError):
            service.create_switch(
                {
                    "id": "BACKDATED", "company": "C", "out": "OLD",
                    "incoming": "NEXT", "meter_ids": ["M1"],
                    "planned": "2027-02-28T00:00:00+08:00",
                }
            )

        later = service.create_switch(
            {
                "id": "LATER", "company": "C", "out": "NEW",
                "incoming": "NEXT", "meter_ids": ["M1"],
                "planned": "2027-03-05T00:00:00+08:00", "agreed": True,
            }
        )
        self.assertEqual(later["attribution_status"], "determined")
        self.assertTrue(later["commitment_issued"])
        self.assertEqual(
            service.ownership_at("M1", "2027-03-02T00:00:00+08:00")["owner"], "NEW"
        )


class CorrectionFinanceTest(unittest.TestCase):
    def build_issued_bills(self):
        service = SwitchService(":memory:")
        service.upsert_meter({"id": "M1", "company": "C", "current": "OLD", "zone": "Z"})
        service.store.put("tariff", "T1", {"id": "T1", "retailer": "OLD", "zones": ["Z"], "cents_per_kwh": 50})
        service.store.put("tariff", "T2", {"id": "T2", "retailer": "NEW", "zones": ["Z"], "cents_per_kwh": 63})
        service.create_switch({
            "id": "CH", "company": "C", "out": "OLD", "incoming": "NEW",
            "group": "separate", "meter_ids": ["M1"],
            "planned": "2027-01-02T00:00:00+08:00", "agreed": True,
        })
        service.ingest_events({"events": [{
            "change_id": "CH", "meter_id": "M1", "status": "applied",
            "at": "2027-01-02T00:00:00+08:00", "revision": 2, "reading_id": "B1",
        }]}, actor={"type": "meter"})
        for reading in [
            {"id": "A", "meter": "M1", "time": "2027-01-01T00:00:00+08:00", "kwh": 100, "quality": "confirmed"},
            {"id": "B1", "meter": "M1", "time": "2027-01-02T00:00:00+08:00", "kwh": 150, "quality": "confirmed", "revision": 1},
            {"id": "C", "meter": "M1", "time": "2027-01-03T00:00:00+08:00", "kwh": 300, "quality": "confirmed"},
        ]:
            service.ingest_reading(reading, actor={"type": "meter"})
        bills = service.generate_bills("M1")
        for bill in bills:
            service.issue_bill(bill["id"], actor={"type": "retailer", "id": bill["retailer"]})
        return service

    def test_issued_bill_correction_creates_two_claimable_sides(self):
        service = self.build_issued_bills()
        result = service.ingest_reading({
            "id": "B2", "meter": "M1", "time": "2027-01-02T00:00:00+08:00",
            "kwh": 163, "quality": "confirmed", "revision": 2, "revises": "B1",
        }, actor={"type": "meter"})
        self.assertEqual(len(result["adjustment_ids"]), 2)
        adjustments = service.list_adjustments(actor={"type": "system"})
        by_retailer = {a["retailer"]: a for a in adjustments}
        self.assertEqual((by_retailer["OLD"]["delta_kwh"], by_retailer["OLD"]["delta_cents"]), (13, 650))
        self.assertEqual((by_retailer["NEW"]["delta_kwh"], by_retailer["NEW"]["delta_cents"]), (-13, -819))

        with self.assertRaises(PermissionDeniedError):
            service.claim_adjustment(
                by_retailer["OLD"]["id"], actor={"type": "retailer", "id": "NEW"}
            )
        service.acknowledge_adjustment(
            by_retailer["OLD"]["id"], actor={"type": "retailer", "id": "OLD"}
        )
        service.acknowledge_adjustment(
            by_retailer["NEW"]["id"], actor={"type": "retailer", "id": "NEW"}
        )
        boundary_id = service.latest_boundary("M1")["id"]
        recon = service.reconciliation_status(boundary_id)
        self.assertEqual(recon["status"], "balanced")
        self.assertEqual(service.ownership_at("M1", "2027-01-02T01:00:00+08:00")["owner"], "NEW")

    def test_finance_only_sees_own_rates_and_meters(self):
        service = self.build_issued_bills()
        old_view = service.finance_view(actor={"type": "finance", "id": "OLD", "retailer": "OLD"})
        self.assertEqual(old_view["retailer"], "OLD")
        self.assertTrue(all(b["retailer"] == "OLD" for b in old_view["bills"]))
        self.assertTrue(all(t["retailer"] == "OLD" for t in old_view["tariffs"]))
        self.assertEqual({m["id"] for m in old_view["meters"]}, {"M1"})

        new_view = service.finance_view(actor={"type": "retailer", "id": "NEW"})
        self.assertTrue(all(b["retailer"] == "NEW" for b in new_view["bills"]))
        self.assertTrue(all(t["retailer"] == "NEW" for t in new_view["tariffs"]))

    def test_estimated_reading_cannot_settle(self):
        service = SwitchService(":memory:")
        service.upsert_meter({"id": "M2", "company": "C", "current": "OLD", "zone": "Z"})
        service.store.put("tariff", "T", {"id": "T", "retailer": "OLD", "zones": ["Z"], "cents_per_kwh": 50})
        for r in [
            {"id": "R1", "meter": "M2", "time": "2027-01-01T00:00:00+08:00", "kwh": 100, "quality": "confirmed"},
            {"id": "R2", "meter": "M2", "time": "2027-01-02T00:00:00+08:00", "kwh": 120, "quality": "estimated"},
            {"id": "R3", "meter": "M2", "time": "2027-01-03T00:00:00+08:00", "kwh": 150, "quality": "confirmed"},
        ]:
            service.ingest_reading(r, actor={"type": "meter"})
        bills = service.generate_bills("M2")
        self.assertEqual(bills[0]["total_kwh"], 0)
        self.assertEqual(len(bills[0]["provisional_lines"]), 2)
        with self.assertRaises(Exception):
            service.issue_bill(bills[0]["id"], actor={"type": "retailer", "id": "OLD"})


class CommercialTimelineTest(unittest.TestCase):
    def test_both_group_bridge_until_last_success_then_formal(self):
        service = SwitchService(":memory:")
        for mid in ("A", "B"):
            service.upsert_meter({"id": mid, "company": "C", "current": "OLD", "zone": "Z"})
        service.store.put("tariff", "T0", {"id": "T0", "retailer": "OLD", "zones": ["Z"], "cents_per_kwh": 50})
        service.store.put("tariff", "T1", {"id": "T1", "retailer": "NEW", "zones": ["Z"], "cents_per_kwh": 63})
        service.create_switch({
            "id": "G", "company": "C", "out": "OLD", "incoming": "NEW",
            "group": "both", "meter_ids": ["A", "B"],
            "planned": "2027-02-02T00:00:00+08:00",
            "bridge_terms": {"owner": "OLD", "cents_per_kwh": 55}, "agreed": True,
        })
        readings = [
            ("A0", "A", "2027-02-01T00:00:00+08:00", 100),
            ("A1", "A", "2027-02-02T00:00:00+08:00", 110),
            ("A2", "A", "2027-02-03T00:00:00+08:00", 120),
            ("A3", "A", "2027-02-04T00:00:00+08:00", 140),
            ("B0", "B", "2027-02-01T00:00:00+08:00", 1000),
            ("B2", "B", "2027-02-03T00:00:00+08:00", 1030),
        ]
        for rid, mid, at, kwh in readings:
            service.ingest_reading(
                {"id": rid, "meter": mid, "time": at, "kwh": kwh, "quality": "confirmed"},
                actor={"type": "meter"},
            )

        service.ingest_events({"events": [{
            "change_id": "G", "meter_id": "A", "status": "applied",
            "at": "2027-02-02T00:00:00+08:00", "revision": 2, "reading_id": "A1",
        }]}, actor={"type": "meter"})
        partial = service.get_switch("G")
        self.assertEqual(partial["state"], "partially_applied")
        self.assertEqual(partial["commercial_state"], "bridging")
        self.assertEqual(partial["point_states"]["A"]["business_state"], "bridging")

        service.ingest_events({"events": [{
            "change_id": "G", "meter_id": "B", "status": "applied",
            "at": "2027-02-03T00:00:00+08:00", "revision": 3, "reading_id": "B2",
        }]}, actor={"type": "meter"})
        completed = service.get_switch("G")
        self.assertEqual(completed["state"], "confirmed")
        self.assertEqual(completed["formal_commercial_at"], "2027-02-03T00:00:00+08:00")

        entries = service.calculate_settlement("A")["entries"]
        self.assertEqual([(e["kwh"], e["rate_kind"], e["cents_per_kwh"]) for e in entries], [
            (10, "tariff", 50),
            (10, "bridge", 55),
            (20, "tariff", 63),
        ])

    def test_accepted_past_plan_is_delayed_but_owner_remains_original(self):
        service = SwitchService(":memory:")
        service.upsert_meter({"id": "M", "company": "C", "current": "OLD", "zone": "Z"})
        service.store.put("tariff", "T", {"id": "T", "retailer": "OLD", "zones": ["Z"], "cents_per_kwh": 50})
        service.store.put("tariff", "TN", {"id": "TN", "retailer": "NEW", "zones": ["Z"], "cents_per_kwh": 63})
        service.create_switch({
            "id": "D", "company": "C", "out": "OLD", "incoming": "NEW",
            "meter_ids": ["M"], "planned": "2027-02-02T00:00:00+08:00", "agreed": True,
        })
        service.ingest_events({"events": [{
            "change_id": "D", "meter_id": "M", "status": "accepted",
            "at": "2027-02-01T10:00:00+08:00", "revision": 1,
        }]}, actor={"type": "meter"})
        delayed = service.get_switch("D", now="2027-02-03T09:00:00+08:00")
        self.assertEqual(delayed["state"], "delayed")
        self.assertEqual(
            service.ownership_at("M", "2027-02-03T09:00:00+08:00")["owner"], "OLD"
        )

    def test_unpaid_previous_bill_does_not_block_new_application(self):
        case = object.__new__(CorrectionFinanceTest)
        service = case.build_issued_bills()
        # 账单已签发但没有任何支付/结清动作；仍允许就更晚期间申请下一家。
        service.store.put("tariff", "T3", {
            "id": "T3", "retailer": "THIRD", "zones": ["Z"], "cents_per_kwh": 61,
        })
        new_request = service.create_switch({
            "id": "THIRD", "company": "C", "out": "NEW", "incoming": "THIRD",
            "meter_ids": ["M1"], "planned": "2027-03-01T00:00:00+08:00",
            "agreed": True,
        })
        self.assertEqual(new_request["attribution_status"], "determined")
        self.assertTrue(new_request["commitment_issued"])


class PersistenceAndApiTest(unittest.TestCase):
    def test_sqlite_restart_keeps_facts_and_idempotency(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "ledger.sqlite3"
            service = SwitchService(db, data_dir=DATA)
            feed_ch14_events(service)
            service.store.close()

            restarted = SwitchService(db)
            self.assertEqual(restarted.get_switch("CH-14")["applied_meter_ids"], ["E-1"])
            before = len(restarted.store.list("meter_event"))
            feed_ch14_events(restarted)
            after = len(restarted.store.list("meter_event"))
            self.assertEqual(before, after)

    def test_api_dispatch_and_customer_trace(self):
        service = SwitchService(":memory:", data_dir=DATA)
        api = Api(service)
        status, health = api.dispatch("GET", "/health")
        self.assertEqual(status, 200)
        self.assertTrue(health["ok"])

        status, created = api.dispatch(
            "POST",
            "/api/agreements",
            {
                "id": "MA1",
                "customer": "桃源板材",
                "meter_ids": ["E-1", "E-2"],
            },
            {"X-Actor-Type": "customer", "X-Actor-Id": "桃源板材"},
        )
        self.assertEqual(status, 201, created)

        status, result = api.dispatch(
            "POST",
            "/api/switches",
            {
                "id": "API-1",
                "company": "桃源板材",
                "out": "松江零售",
                "incoming": "岚岭售电",
                "group": "both",
                "master_agreement_id": "MA1",
                "planned": "2027-03-01T00:00:00+08:00",
                "bridge_terms": {"owner": "松江零售", "cents_per_kwh": 55},
                "agreed": True,
            },
            {"X-Actor-Type": "customer", "X-Actor-Id": "桃源板材"},
        )
        self.assertEqual(status, 201, result)
        self.assertEqual(result["meter_ids"], ["E-1", "E-2"])

        feed_ch14_events(service)
        status, trace = api.dispatch("GET", "/api/trace/E-1", headers={
            "X-Actor-Type": "customer", "X-Actor-Id": "桃源板材"
        })
        self.assertEqual(status, 200)
        self.assertTrue(trace["boundaries"])
        self.assertTrue(trace["links"][0]["voucher_id"])

        status, _ = api.dispatch("GET", "/api/switches")
        self.assertEqual(status, 401)
        finance_headers = {
            "X-Actor-Type": "finance",
            "X-Actor-Id": "松江零售",
            "X-Actor-Retailer": "松江零售",
        }
        status, scoped = api.dispatch(
            "GET", "/api/settlement?meter_id=E-1", headers=finance_headers
        )
        self.assertEqual(status, 200)
        self.assertEqual(scoped["retailer"], "松江零售")
        self.assertTrue(all(e["retailer"] == "松江零售" for e in scoped["entries"]))
        status, _ = api.dispatch(
            "GET", "/api/readings?meter_id=E-1", headers=finance_headers
        )
        self.assertEqual(status, 403)


class PeerHttpIntegrationTest(unittest.TestCase):
    def test_apply_poll_and_dropped_withdrawal_reply(self):
        from peer import build_server

        script = DATA / "peer.json"
        server = build_server(script)
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        tmp = tempfile.TemporaryDirectory()
        try:
            db = Path(tmp.name) / "peer.sqlite3"
            service = SwitchService(
                db, data_dir=DATA, peer_url=f"http://127.0.0.1:{server.server_port}"
            )
            accepted = service.submit_to_meter("CH-14", actor={"type": "system"})
            self.assertEqual(accepted["point_states"]["E-1"]["state"], "accepted")

            applied = service.poll_meter("CH-14")
            self.assertEqual(applied["point_states"]["E-1"]["state"], "confirmed")
            self.assertEqual(applied["point_states"]["E-2"]["state"], "declined")

            withdrawn = service.request_withdrawal("CH-14", actor={"type": "system"})
            self.assertEqual(withdrawn["point_states"]["E-1"]["state"], "confirmed")
            self.assertEqual(withdrawn["point_states"]["E-2"]["state"], "declined")
            self.assertTrue(withdrawn["withdrawal"]["requested_at"])
            service.store.close()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=3)
            tmp.cleanup()


if __name__ == "__main__":
    unittest.main()
