"""重放联调方的 HTTP 报文。"""
import argparse
import json
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import socket
from threading import Lock


def build_server(script_path, port=0):
    records = json.loads(script_path.read_text(encoding="utf-8"))
    exchanges = {record["path"]: record for record in records}
    read_counts = {}
    lock = Lock()

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, value):
            data = json.dumps(value, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            exchange = exchanges.get(self.path)
            if exchange is None:
                self.reply(HTTPStatus.NOT_FOUND, {"error": "没有该联调请求"})
                return
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if not 0 < size <= 65536:
                    raise ValueError
                request = json.loads(self.rfile.read(size))
            except (ValueError, UnicodeError):
                self.reply(HTTPStatus.BAD_REQUEST, {"error": "需要有效的 JSON 请求"})
                return
            if request != exchange["expect"]:
                self.reply(HTTPStatus.CONFLICT, {"error": "联调请求与约定不符"})
                return
            with lock:
                first = self.path not in read_counts
                index = read_counts.setdefault(self.path, 0)
            if first and exchange["drop_first_reply"]:
                self.connection.shutdown(socket.SHUT_RDWR)
                self.close_connection = True
                return
            self.reply(HTTPStatus.OK, exchange["responses"][index])

        def do_GET(self):
            with lock:
                if self.path in read_counts:
                    responses = exchanges[self.path]["responses"]
                    index = min(len(responses) - 1, read_counts[self.path] + 1)
                    read_counts[self.path] = index
                    value = responses[index]
                else:
                    value = None
            if value is None:
                self.reply(HTTPStatus.NOT_FOUND, {"error": "未收到该联调请求"})
                return
            self.reply(HTTPStatus.OK, value)

    return ThreadingHTTPServer(("127.0.0.1", port), Handler)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="在本地重放外方联调报文")
    parser.add_argument("--port", type=int, default=8412)
    parser.add_argument("--script", type=Path, default=Path("data/peer.json"))
    args = parser.parse_args()
    with build_server(args.script, args.port) as server:
        print(f"联调地址 http://127.0.0.1:{server.server_port}", flush=True)
        server.serve_forever()
