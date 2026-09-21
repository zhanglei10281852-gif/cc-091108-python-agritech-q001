"""仅依赖标准库的 JSON HTTP 服务。

路由：
  POST /telemetry                 接入遥测（乱序/重复安全）
  POST /plans/preview             预演（不落库、不占额）
  POST /plans/drafts              生成/替换当日草案
  POST /plans/{id}/publish        发布草案
  POST /plans/revise              修订并重新发布（body: {day, et0, thresholds,...}）
  GET  /plans?day=YYYY-MM-DD      查看当日生效计划
  GET  /plans/{id}
  POST /tick                      推进调度（测试/手动）
  POST /reconcile                 重启对账（重投待确认命令）
  POST /commands/{uid}/ack        阀门回执（幂等）
  POST /entries/{id}/resolve      人工处置失联关阀
  POST /pause / POST /resume      人工停灌/恢复
  GET  /dashboard?day=            主管看板
  GET  /ledger?day=               水量台账
  GET  /health
"""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from . import timeutil
from .service import IrrigationService


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "IrrigationService/1.0"

    # ---- 工具 ----
    def _json(self, code: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _now(self, body: dict):
        if body.get("now"):
            return timeutil.parse(body["now"], self.service.cfg.timezone)
        return None

    @property
    def service(self) -> IrrigationService:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # 简化日志
        self.server.access_log.append(f"{self.address_string()} - {fmt % args}")  # type: ignore

    # ---- 路由 ----
    def do_GET(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.strip("/")
        q = parse_qs(parsed.query)
        try:
            if path == "health":
                return self._json(200, {"ok": True})
            if path == "dashboard":
                return self._json(200, self.service.dashboard(q.get("day", [None])[0]))
            if path == "ledger":
                return self._json(200, self.service.ledger(q.get("day", [None])[0]))
            if path == "plans":
                day = q.get("day", [None])[0]
                if day:
                    row = self.service.store.get_active_plan(day)
                    if row is None:
                        return self._json(404, {"error": f"{day} 无生效计划"})
                    return self._json(200, self.service.get_plan(row["id"]))
                return self._json(400, {"error": "需要 day 参数"})
            if path.startswith("plans/"):
                plan_id = path.split("/", 1)[1]
                return self._json(200, self.service.get_plan(plan_id))
            return self._json(404, {"error": "not found"})
        except KeyError as exc:
            return self._json(404, {"error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            return self._json(500, {"error": str(exc)})

    def do_POST(self):  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.strip("/")
        parts = path.split("/")
        try:
            body = self._body()
            now = self._now(body)
            svc = self.service

            if path == "telemetry":
                return self._json(200, svc.ingest_telemetry(body.get("events", []), now))
            if path == "plans/preview":
                return self._json(200, svc.build_plan(
                    body["day"], et0=body.get("et0"), windows=body.get("windows"),
                    thresholds=body.get("thresholds"), targets=body.get("targets"),
                    note=body.get("note", ""), now=now,
                ))
            if path == "plans/drafts":
                return self._json(201, svc.build_plan(
                    body["day"], et0=body.get("et0"), windows=body.get("windows"),
                    thresholds=body.get("thresholds"), targets=body.get("targets"),
                    persist=True, note=body.get("note", ""), now=now,
                ))
            if len(parts) == 3 and parts[0] == "plans" and parts[2] == "publish":
                return self._json(200, svc.publish_plan(parts[1], now=now))
            if path == "plans/revise":
                return self._json(200, svc.revise_plan(
                    body["day"], et0=body.get("et0"), windows=body.get("windows"),
                    thresholds=body.get("thresholds"), targets=body.get("targets"),
                    note=body.get("note", "农艺师修订"),
                    publish=body.get("publish", True), now=now,
                ))
            if path == "tick":
                return self._json(200, svc.tick(now))
            if path == "reconcile":
                return self._json(200, svc.reconcile(now))
            if len(parts) == 3 and parts[0] == "commands" and parts[2] == "ack":
                return self._json(200, svc.ack_command(
                    parts[1], bool(body.get("ok", True)), body.get("result"), now))
            if len(parts) == 3 and parts[0] == "entries" and parts[2] == "resolve":
                return self._json(200, svc.resolve_entry(
                    parts[1], body["decision"], body.get("measured_l"),
                    operator=body.get("operator", "operator"), now=now))
            if path == "pause":
                return self._json(200, svc.pause(
                    scope=body.get("scope", "global"), zone_id=body.get("zone_id"),
                    reason=body.get("reason", ""),
                    operator=body.get("operator", "operator"), now=now))
            if path == "resume":
                return self._json(200, svc.resume(
                    scope=body.get("scope", "global"), zone_id=body.get("zone_id"),
                    operator=body.get("operator", "operator"), now=now))
            return self._json(404, {"error": "not found"})
        except KeyError as exc:
            return self._json(404, {"error": str(exc)})
        except (ValueError, TypeError) as exc:
            return self._json(400, {"error": str(exc)})
        except Exception as exc:  # noqa: BLE001
            return self._json(500, {"error": str(exc)})


def create_server(service: IrrigationService, host: str = "127.0.0.1",
                  port: int = 8080) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), ApiHandler)
    server.service = service  # type: ignore[attr-defined]
    server.access_log = []  # type: ignore[attr-defined]
    return server


def serve_forever(service: IrrigationService, host: str, port: int,
                  tick_interval: float = 10.0) -> None:
    server = create_server(service, host, port)
    stop = threading.Event()

    def _ticker():
        service.reconcile()
        while not stop.wait(tick_interval):
            service.tick()

    t = threading.Thread(target=_ticker, name="scheduler", daemon=True)
    t.start()
    try:
        server.serve_forever()
    finally:
        stop.set()
        server.server_close()
