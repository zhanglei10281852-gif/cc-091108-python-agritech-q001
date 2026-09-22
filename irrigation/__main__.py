"""服务启动入口。

用法：
    python -m irrigation --domain reference/domain.json --state data/events.jsonl \
        --host 0.0.0.0 --port 8080

首次启动若状态文件为空，会自动把资料包 telemetry 作为历史读数导入；
之后每次启动从事件日志重放，从同一计划继续。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .api import make_server
from .config import load_domain
from .service import IrrigationService
from .store import EventStore


def build_service(domain_path: str, state_path: str) -> IrrigationService:
    domain = load_domain(domain_path)
    store = EventStore(state_path)
    service = IrrigationService(domain, store)
    if service._replayed == 0:  # noqa: SLF001
        # 冷启动：灌入资料包中的乱序遥测作为领域约定样例
        raw = json.loads(Path(domain_path).read_text(encoding="utf-8"))
        service.ingest_readings(raw.get("telemetry", []))
    return service


def main() -> None:
    parser = argparse.ArgumentParser(description="温室灌溉决策服务")
    parser.add_argument("--domain", default="reference/domain.json")
    parser.add_argument("--state", default="data/events.jsonl")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    service = build_service(args.domain, args.state)
    server = make_server(service, args.host, args.port)
    print(f"灌溉决策服务已启动: http://{args.host}:{args.port}")
    print(f"领域配置: {args.domain}  事件日志: {args.state}  已重放 {service._replayed} 条事件")  # noqa: SLF001
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n收到中断，关闭服务")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
