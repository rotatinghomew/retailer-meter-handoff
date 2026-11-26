import json
import sys
import tempfile
import unittest
from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class SourceTest(unittest.TestCase):
    def rows(self, name):
        return json.loads((ROOT / "data" / (name + ".json")).read_text(encoding="utf-8"))

    def test_record_ids(self):
        ids = []
        for file in (ROOT / "data").glob("*.json"):
            if file.name == "peer.json":
                continue
            ids.extend(row["id"] for row in self.rows(file.stem))
        self.assertTrue(ids)
        self.assertEqual(len(ids), len(set(ids)))

    def test_local_ledger_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            database = Path(tmp) / "probe.sqlite3"
            with sqlite3.connect(database) as db:
                db.execute("CREATE TABLE probe (amount INTEGER NOT NULL)")
                db.execute("INSERT INTO probe VALUES (93)")
            with sqlite3.connect(database) as db:
                self.assertEqual(db.execute("SELECT amount FROM probe").fetchone(), (93,))

    def test_links_in_exchange_records(self):
        a, b = self.rows("requests")
        self.assertEqual(a["out"], b["out"])
        self.assertTrue(set(a["meter_ids"]) & set(b["meter_ids"]))
        old, new = self.rows("reads")[1:3]
        self.assertEqual((old["meter"], old["time"]), (new["meter"], new["time"]))
        self.assertNotEqual(old["kwh"], new["kwh"])


class PeerTest(unittest.TestCase):
    def test_request_and_reads(self):
        from http.client import RemoteDisconnected
        from threading import Thread
        from urllib.error import HTTPError
        from urllib.request import Request, urlopen

        from peer import build_server

        script = ROOT / "data" / "peer.json"
        with build_server(script) as server:
            thread = Thread(target=server.serve_forever)
            thread.start()
            try:
                for exchange in json.loads(script.read_text(encoding="utf-8")):
                    url = f"http://127.0.0.1:{server.server_port}{exchange['path']}"
                    with self.assertRaises(HTTPError) as not_found:
                        urlopen(url, timeout=3)
                    self.assertEqual(not_found.exception.code, 404)
                    payload = json.dumps(exchange["expect"]).encode("utf-8")
                    request = Request(url, payload, headers={"Content-Type": "application/json"})
                    if exchange["drop_first_reply"]:
                        with self.assertRaises(RemoteDisconnected):
                            urlopen(request, timeout=3)
                    else:
                        with urlopen(request, timeout=3) as response:
                            self.assertEqual(json.load(response), exchange["responses"][0])
                    for expected in exchange["responses"][1:] or exchange["responses"]:
                        with urlopen(url, timeout=3) as response:
                            self.assertEqual(json.load(response), expected)
                    with urlopen(request, timeout=3) as response:
                        self.assertEqual(json.load(response), exchange["responses"][-1])
            finally:
                server.shutdown()
                thread.join()
