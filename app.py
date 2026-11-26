"""WSGI/ASGI 无关的应用入口。

直接运行可启动标准库 HTTP 服务：``python app.py --port 8410``。
作为 WSGI 应用使用时，``app`` / ``application`` 均可调用。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from wsgiref.simple_server import make_server

from service import ROOT, Api, SwitchService, build_http_server


class TestResponse:
    def __init__(self, status_code: int, payload: dict[str, Any]):
        self.status_code = status_code
        self._json = payload
        self.data = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def get_json(self) -> dict[str, Any]:
        return self._json

    @property
    def json(self) -> dict[str, Any]:
        return self._json


class TestClient:
    """极小的 requests/Flask test_client 风格封装，供无 Socket 验收使用。"""

    def __init__(self, app: "JsonWsgiApp"):
        self.app = app

    def _request(self, method: str, path: str, payload: dict[str, Any] | None, headers: dict[str, str] | None):
        status, body = self.app.api.dispatch(method, path, payload or {}, headers or {})
        return TestResponse(status, body)

    def get(self, path: str, headers: dict[str, str] | None = None):
        return self._request("GET", path, None, headers)

    def post(self, path: str, json: dict[str, Any] | None = None, headers: dict[str, str] | None = None, **kwargs: Any):
        payload = json if json is not None else kwargs.get("data") or {}
        return self._request("POST", path, payload, headers)


class JsonWsgiApp:
    def __init__(
        self,
        db_path: str | Path | None = ":memory:",
        *,
        data_dir: str | Path | None = ROOT / "data",
        peer_url: str | None = None,
    ):
        self.service = SwitchService(db_path, data_dir=data_dir, peer_url=peer_url)
        self.api = Api(self.service)

    def test_client(self) -> TestClient:
        return TestClient(self)

    def __call__(self, environ: dict[str, Any], start_response: Any):
        method = environ.get("REQUEST_METHOD", "GET")
        path = environ.get("PATH_INFO", "/")
        query = environ.get("QUERY_STRING", "")
        if query:
            path = path + "?" + query
        headers = {
            key[5:].replace("_", "-").title(): value
            for key, value in environ.items()
            if key.startswith("HTTP_")
        }
        length = int(environ.get("CONTENT_LENGTH") or 0)
        payload: dict[str, Any] = {}
        if length:
            raw = environ["wsgi.input"].read(length)
            payload = json.loads(raw.decode("utf-8"))
        status, body = self.api.dispatch(method, path, payload, headers)
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        start_response(f"{status} " + {200: "OK", 201: "Created", 400: "Bad Request", 403: "Forbidden", 404: "Not Found", 409: "Conflict", 422: "Unprocessable Entity"}.get(status, "OK"), [
            ("Content-Type", "application/json; charset=utf-8"),
            ("Content-Length", str(len(data))),
        ])
        return [data]


def create_app(
    db_path: str | Path | None = None,
    *,
    data_dir: str | Path | None = ROOT / "data",
    peer_url: str | None = None,
) -> JsonWsgiApp:
    db_path = db_path if db_path is not None else os.environ.get("SWITCH_DB", ":memory:")
    peer_url = peer_url if peer_url is not None else os.environ.get("PEER_URL")
    return JsonWsgiApp(db_path, data_dir=data_dir, peer_url=peer_url)


# WSGI 服务器通常寻找 app / application。
app = create_app()
application = app


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="售电切换 JSON 服务（WSGI/标准库）")
    parser.add_argument("--host", default=os.environ.get("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8410")))
    parser.add_argument("--db", default=os.environ.get("SWITCH_DB", "switch_service.sqlite3"))
    parser.add_argument("--data", default=str(ROOT / "data"))
    parser.add_argument("--peer-url", default=os.environ.get("PEER_URL", "http://127.0.0.1:8412"))
    args = parser.parse_args()

    service = SwitchService(args.db, data_dir=args.data, peer_url=args.peer_url)
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
