"""基于标准库的 HTTP 适配层，零第三方依赖即可部署。

路由：
    POST   /readings                 上报遥测（支持批量，自动按 occurred_at 定序）
    POST   /et0?date=YYYY-MM-DD      录入当日参考蒸散
    POST   /plans?date=YYYY-MM-DD    生成预演计划  body: {"et0": 4.2}
    POST   /plans/{id}/thresholds    农艺师修订阈值 body: {"GH-A-01": {"min_moisture": 0.2}}
    POST   /preview?date=YYYY-MM-DD  用水预演（不落状态）
    POST   /plans/{id}/publish       发布计划
    POST   /plans/{id}/revise        吸收迟到读数重算未锁定时段
    GET    /plans/{id}               计划明细（为何浇、依据哪条读数）
    POST   /tick                     控制器节拍：到时签发 + 失联巡检 body: {"at": "..."}
    POST   /receipts                 阀门回执（重复执据幂等）
    POST   /manual-stop              人工停灌 body: {"valve_id": "V-01", "reason": "..."}
    POST   /valves/{id}/resume       解除人工接管
    GET    /commands/pending         待确认命令
    GET    /quota?date=YYYY-MM-DD    当日配额台账
    GET    /zones                    各区状态（为何浇、当日安排）
    GET    /status                   主管总览
"""

from __future__ import annotations

import json
from datetime import date as date_cls
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .clock import parse
from .service import IrrigationService, Receipt


class _Handler(BaseHTTPRequestHandler):
    service: IrrigationService = None  # 由 make_server 注入

    def log_message(self, fmt, *args):  # 静音默认访问日志
        return

    def _send(self, code: int, payload) -> None:
        body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _at(self, body: dict):
        return parse(body["at"]) if body.get("at") else None

    def do_GET(self):  # noqa: N802
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        svc = self.service
        try:
            if url.path == "/status":
                return self._send(200, svc.status())
            if url.path == "/zones":
                return self._send(200, {"zones": svc.zone_status()})
            if url.path == "/commands/pending":
                return self._send(200, {"commands": svc.pending_commands()})
            if url.path == "/quota":
                day = date_cls.fromisoformat(q["date"]) if "date" in q else None
                return self._send(200, svc.quota_view(day))
            if url.path.startswith("/plans/"):
                return self._send(200, svc.plan_view(url.path.rsplit("/", 1)[-1]))
            self._send(404, {"error": "not_found", "path": url.path})
        except KeyError as exc:
            self._send(404, {"error": "not_found", "detail": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._send(400, {"error": type(exc).__name__, "detail": str(exc)})

    def do_POST(self):  # noqa: N802
        url = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        svc = self.service
        try:
            body = self._body()
            if url.path == "/readings":
                items = body if isinstance(body, list) else [body]
                readings = svc.ingest_readings(items)
                return self._send(200, {"ingested": [r.event_id for r in readings]})
            if url.path == "/et0":
                day = date_cls.fromisoformat(q.get("date", svc.now().date().isoformat()))
                svc.set_et0(day, body.get("et0", body))
                return self._send(200, {"ok": True})
            if url.path == "/plans":
                day = date_cls.fromisoformat(q["date"])
                plan = svc.generate_plan(day, body.get("et0"), replace=bool(body.get("replace")))
                return self._send(201, svc.plan_view(plan.plan_id))
            if url.path == "/preview":
                day = date_cls.fromisoformat(q["date"])
                return self._send(200, svc.preview(day, body.get("et0"), body.get("thresholds")))
            if url.path == "/tick":
                at = self._at(body)
                return self._send(200, {**svc.dispatch_due(at), **svc.reconcile(at)})
            if url.path == "/receipts":
                rec = Receipt(
                    receipt_id=body["receipt_id"], command_id=body["command_id"],
                    kind=body["kind"], at=parse(body["at"]),
                    observed_lpm=body.get("observed_lpm"),
                )
                return self._send(200, svc.record_receipt(rec))
            if url.path == "/manual-stop":
                return self._send(200, svc.manual_stop(
                    valve_id=body.get("valve_id"), zone_id=body.get("zone_id"),
                    at=self._at(body), reason=body.get("reason", "operator_stop")))
            parts = [p for p in url.path.split("/") if p]
            if len(parts) == 3 and parts[0] == "plans" and parts[2] == "publish":
                return self._send(200, svc.plan_view(svc.publish_plan(parts[1]).plan_id))
            if len(parts) == 3 and parts[0] == "plans" and parts[2] == "revise":
                return self._send(200, svc.revise_plan(parts[1]))
            if len(parts) == 3 and parts[0] == "plans" and parts[2] == "thresholds":
                svc.update_thresholds(parts[1], body)
                return self._send(200, svc.plan_view(parts[1]))
            if len(parts) == 3 and parts[0] == "valves" and parts[2] == "resume":
                svc.resume_auto(parts[1])
                return self._send(200, {"ok": True})
            self._send(404, {"error": "not_found", "path": url.path})
        except KeyError as exc:
            self._send(404, {"error": "not_found", "detail": str(exc)})
        except Exception as exc:  # noqa: BLE001
            self._send(400, {"error": type(exc).__name__, "detail": str(exc)})


def make_server(service: IrrigationService, host: str = "127.0.0.1", port: int = 8080):
    handler = type("ServiceHandler", (_Handler,), {"service": service})
    server = ThreadingHTTPServer((host, port), handler)
    server.service = service
    return server
