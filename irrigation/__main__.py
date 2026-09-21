"""命令行入口：

  python -m irrigation serve --db data/irr.db --reference reference/domain.json --port 8080
  python -m irrigation tick  --db data/irr.db
"""
from __future__ import annotations

import argparse

from .config import DomainConfig
from .service import IrrigationService
from .store import Store
from .api import serve_forever


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="温室灌溉决策服务")
    sub = parser.add_subparsers(dest="cmd", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default="data/irr.db")
    common.add_argument("--reference", default="reference/domain.json")

    s = sub.add_parser("serve", parents=[common])
    s.add_argument("--host", default="0.0.0.0")
    s.add_argument("--port", type=int, default=8080)
    s.add_argument("--tick-interval", type=float, default=10.0)

    t = sub.add_parser("tick", parents=[common])
    t.add_argument("--reconcile", action="store_true")

    args = parser.parse_args(argv)
    cfg = DomainConfig.load(args.reference)
    store = Store(args.db)
    service = IrrigationService(store, cfg)
    try:
        if args.cmd == "serve":
            print(f"灌溉决策服务监听 http://{args.host}:{args.port}")
            serve_forever(service, args.host, args.port, args.tick_interval)
        elif args.cmd == "tick":
            if args.reconcile:
                print(service.reconcile())
            print(service.tick())
    finally:
        store.close()


if __name__ == "__main__":
    main()
