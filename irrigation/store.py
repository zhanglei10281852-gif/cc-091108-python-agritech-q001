from __future__ import annotations

import json
import sqlite3
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY, value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS plans (
  id TEXT PRIMARY KEY,
  plan_date TEXT NOT NULL,
  revision INTEGER NOT NULL,
  revision_of TEXT,
  status TEXT NOT NULL,          -- draft / published / superseded
  et0_json TEXT NOT NULL,
  windows_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  published_at TEXT,
  note TEXT
);
CREATE TABLE IF NOT EXISTS plan_zones (
  plan_id TEXT NOT NULL,
  zone_id TEXT NOT NULL,
  moisture_threshold REAL,
  moisture_target REAL,
  moisture REAL,
  quality TEXT,
  et0_mm REAL,
  kc REAL,
  et_need_l REAL,
  deficit_l REAL,
  demand_l REAL,
  scheduled_l REAL DEFAULT 0,
  unscheduled_l REAL DEFAULT 0,
  reason TEXT,
  recipe_json TEXT,
  PRIMARY KEY (plan_id, zone_id)
);
CREATE TABLE IF NOT EXISTS entries (
  id TEXT PRIMARY KEY,
  plan_id TEXT NOT NULL,
  zone_id TEXT NOT NULL,
  valve_id TEXT NOT NULL,
  seq INTEGER NOT NULL,
  start_at TEXT NOT NULL,
  end_at TEXT NOT NULL,
  planned_l REAL NOT NULL,
  day_split_json TEXT NOT NULL,   -- [{day, seconds, liters}]
  status TEXT NOT NULL,           -- pending/dispatched/closed/confirmed/cancelled/blocked
  actual_l REAL,
  actual_split_json TEXT,
  settle_source TEXT,             -- measured / rated
  UNIQUE (plan_id, zone_id, seq)
);
CREATE TABLE IF NOT EXISTS commands (
  command_uid TEXT PRIMARY KEY,
  entry_id TEXT NOT NULL,
  valve_id TEXT NOT NULL,
  action TEXT NOT NULL,           -- open / close
  at TEXT NOT NULL,
  payload_json TEXT,
  ack_at TEXT,
  ok INTEGER,
  result_json TEXT,
  status TEXT NOT NULL DEFAULT 'pending',  -- pending / acked
  dup_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS ledger (
  day TEXT NOT NULL,
  ref_kind TEXT NOT NULL,         -- reserve / release / consume
  ref_key TEXT NOT NULL,
  zone_id TEXT,
  liters REAL NOT NULL,
  note TEXT,
  at TEXT NOT NULL,
  PRIMARY KEY (day, ref_kind, ref_key)
);
CREATE TABLE IF NOT EXISTS telemetry (
  event_id TEXT PRIMARY KEY,
  zone_id TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  received_at TEXT NOT NULL,
  moisture REAL NOT NULL,
  quality TEXT NOT NULL,
  incorporated INTEGER NOT NULL DEFAULT 0,
  incorporated_at TEXT
);
CREATE TABLE IF NOT EXISTS overrides (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  scope TEXT NOT NULL,            -- global / zone
  zone_id TEXT,
  action TEXT NOT NULL,           -- pause / resume
  at TEXT NOT NULL,
  reason TEXT,
  operator TEXT,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS zone_state (
  zone_id TEXT PRIMARY KEY,
  anchored_at TEXT,
  depletion_mm REAL NOT NULL DEFAULT 0,
  last_irrigation_at TEXT,
  last_irrigation_l REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS audit (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  actor TEXT,
  action TEXT NOT NULL,
  detail_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_entries_plan ON entries(plan_id);
CREATE INDEX IF NOT EXISTS idx_entries_status ON entries(status);
CREATE INDEX IF NOT EXISTS idx_commands_entry ON commands(entry_id);
CREATE INDEX IF NOT EXISTS idx_telemetry_zone ON telemetry(zone_id, occurred_at);
"""


class Store:
    """SQLite 持久化。所有写操作在 service 层单锁内串行执行。"""

    def __init__(self, path: str | Path):
        self.path = str(path)
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    # ---- 基础工具 ----
    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def meta_get(self, key: str) -> str | None:
        row = self.conn.execute("SELECT value FROM meta WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def meta_set(self, key: str, value: str) -> None:
        self.conn.execute(
            "INSERT INTO meta(key,value) VALUES(?,?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )

    def audit(self, at: str, actor: str, action: str, detail: dict) -> None:
        self.conn.execute(
            "INSERT INTO audit(at,actor,action,detail_json) VALUES(?,?,?,?)",
            (at, actor, action, json.dumps(detail, ensure_ascii=False)),
        )

    # ---- 计划 ----
    def insert_plan(self, p: dict) -> None:
        self.conn.execute(
            "INSERT INTO plans(id,plan_date,revision,revision_of,status,et0_json,"
            "windows_json,created_at,published_at,note) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                p["id"], p["plan_date"], p["revision"], p.get("revision_of"), p["status"],
                json.dumps(p["et0"], ensure_ascii=False),
                json.dumps(p["windows"], ensure_ascii=False),
                p["created_at"], p.get("published_at"), p.get("note"),
            ),
        )

    def update_plan_status(self, plan_id: str, status: str, published_at: str | None) -> None:
        self.conn.execute(
            "UPDATE plans SET status=?, published_at=COALESCE(?,published_at) WHERE id=?",
            (status, published_at, plan_id),
        )

    def get_plan(self, plan_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM plans WHERE id=?", (plan_id,)).fetchone()

    def get_draft(self, day: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM plans WHERE plan_date=? AND status='draft' ORDER BY revision DESC LIMIT 1",
            (day,),
        ).fetchone()

    def get_active_plan(self, day: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM plans WHERE plan_date=? AND status='published' "
            "ORDER BY revision DESC LIMIT 1",
            (day,),
        ).fetchone()

    def upsert_plan_zone(self, pz: dict) -> None:
        self.conn.execute(
            "INSERT INTO plan_zones(plan_id,zone_id,moisture_threshold,moisture_target,"
            "moisture,quality,et0_mm,kc,et_need_l,deficit_l,demand_l,scheduled_l,"
            "unscheduled_l,reason,recipe_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
            "ON CONFLICT(plan_id,zone_id) DO UPDATE SET "
            "moisture_threshold=excluded.moisture_threshold,"
            "moisture_target=excluded.moisture_target,moisture=excluded.moisture,"
            "quality=excluded.quality,et0_mm=excluded.et0_mm,kc=excluded.kc,"
            "et_need_l=excluded.et_need_l,deficit_l=excluded.deficit_l,"
            "demand_l=excluded.demand_l,scheduled_l=excluded.scheduled_l,"
            "unscheduled_l=excluded.unscheduled_l,reason=excluded.reason,"
            "recipe_json=excluded.recipe_json",
            (
                pz["plan_id"], pz["zone_id"], pz.get("moisture_threshold"),
                pz.get("moisture_target"), pz.get("moisture"), pz.get("quality"),
                pz.get("et0_mm"), pz.get("kc"), pz.get("et_need_l"), pz.get("deficit_l"),
                pz.get("demand_l"), pz.get("scheduled_l"), pz.get("unscheduled_l"),
                json.dumps(pz.get("reason", {}), ensure_ascii=False),
                json.dumps(pz.get("recipe"), ensure_ascii=False) if pz.get("recipe") is not None else None,
            ),
        )

    def plan_zone_rows(self, plan_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM plan_zones WHERE plan_id=?", (plan_id,)
        ).fetchall()

    def plan_zone_row(self, plan_id: str, zone_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM plan_zones WHERE plan_id=? AND zone_id=?", (plan_id, zone_id)
        ).fetchone()

    # ---- 时段 / 命令 ----
    def insert_entry(self, e: dict) -> None:
        self.conn.execute(
            "INSERT INTO entries(id,plan_id,zone_id,valve_id,seq,start_at,end_at,"
            "planned_l,day_split_json,status) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                e["id"], e["plan_id"], e["zone_id"], e["valve_id"], e["seq"],
                e["start_at"], e["end_at"], e["planned_l"],
                json.dumps(e["day_split"], ensure_ascii=False), "pending",
            ),
        )

    def entries_of_plan(self, plan_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM entries WHERE plan_id=? ORDER BY start_at, seq", (plan_id,)
        ).fetchall()

    def get_entry(self, entry_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM entries WHERE id=?", (entry_id,)).fetchone()

    def entries_by_status(self, statuses: list[str]) -> list[sqlite3.Row]:
        q = ",".join("?" * len(statuses))
        return self.conn.execute(
            f"SELECT * FROM entries WHERE status IN ({q}) ORDER BY start_at", statuses
        ).fetchall()

    def update_entry(self, entry_id: str, **fields) -> None:
        if not fields:
            return
        sets = []
        vals: list = []
        for k, v in fields.items():
            if k.endswith("_json"):
                v = json.dumps(v, ensure_ascii=False)
            sets.append(f"{k}=?")
            vals.append(v)
        vals.append(entry_id)
        self.conn.execute(f"UPDATE entries SET {','.join(sets)} WHERE id=?", vals)

    def insert_command(self, c: dict) -> None:
        self.conn.execute(
            "INSERT INTO commands(command_uid,entry_id,valve_id,action,at,payload_json,"
            "status) VALUES(?,?,?,?,?,?, 'pending')",
            (
                c["command_uid"], c["entry_id"], c["valve_id"], c["action"], c["at"],
                json.dumps(c.get("payload", {}), ensure_ascii=False),
            ),
        )

    def get_command(self, uid: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM commands WHERE command_uid=?", (uid,)).fetchone()

    def commands_of_entry(self, entry_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM commands WHERE entry_id=? ORDER BY at", (entry_id,)
        ).fetchall()

    def ack_command(self, uid: str, at: str, ok: bool, result: dict) -> None:
        self.conn.execute(
            "UPDATE commands SET ack_at=?, ok=?, result_json=?, status='acked' "
            "WHERE command_uid=?",
            (at, 1 if ok else 0, json.dumps(result, ensure_ascii=False), uid),
        )

    def bump_dup(self, uid: str) -> None:
        self.conn.execute(
            "UPDATE commands SET dup_count=dup_count+1 WHERE command_uid=?", (uid,)
        )

    def pending_commands(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM commands WHERE status='pending' ORDER BY at"
        ).fetchall()

    # ---- 台账（幂等）----
    def ledger_insert_ignore(self, day: str, kind: str, key: str, zone_id: str,
                             liters: float, at: str, note: str = "") -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO ledger(day,ref_kind,ref_key,zone_id,liters,note,at) "
            "VALUES(?,?,?,?,?,?,?)",
            (day, kind, key, zone_id, liters, note, at),
        )
        return cur.rowcount > 0

    def ledger_adjust_consume(self, day: str, key: str, liters: float) -> None:
        self.conn.execute(
            "UPDATE ledger SET liters=? WHERE day=? AND ref_kind='consume' AND ref_key=?",
            (liters, day, key),
        )

    def ledger_get(self, day: str, kind: str, key: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM ledger WHERE day=? AND ref_kind=? AND ref_key=?",
            (day, kind, key),
        ).fetchone()

    def committed(self, day: str) -> float:
        row = self.conn.execute(
            "SELECT COALESCE(SUM(liters),0) AS s FROM ledger WHERE day=?", (day,)
        ).fetchone()
        return float(row["s"])

    def ledger_rows(self, day: str | None = None) -> list[sqlite3.Row]:
        if day:
            return self.conn.execute(
                "SELECT * FROM ledger WHERE day=? ORDER BY at,rowid", (day,)
            ).fetchall()
        return self.conn.execute(
            "SELECT * FROM ledger ORDER BY at,rowid"
        ).fetchall()

    # ---- 遥测 ----
    def insert_telemetry(self, t: dict) -> bool:
        cur = self.conn.execute(
            "INSERT OR IGNORE INTO telemetry(event_id,zone_id,occurred_at,received_at,"
            "moisture,quality) VALUES(?,?,?,?,?,?)",
            (
                t["event_id"], t["zone_id"], t["occurred_at"], t["received_at"],
                float(t["moisture"]), t["quality"],
            ),
        )
        return cur.rowcount > 0

    def telemetry_for_zone(self, zone_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM telemetry WHERE zone_id=? ORDER BY occurred_at", (zone_id,)
        ).fetchall()

    def mark_incorporated(self, event_ids: list[str], at: str) -> None:
        if not event_ids:
            return
        q = ",".join("?" * len(event_ids))
        self.conn.execute(
            f"UPDATE telemetry SET incorporated=1, incorporated_at=? WHERE event_id IN ({q})",
            [at, *event_ids],
        )

    def unincorporated(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM telemetry WHERE incorporated=0 ORDER BY occurred_at"
        ).fetchall()

    # ---- 人工干预 / 分区状态 ----
    def insert_override(self, scope: str, zone_id: str | None, action: str, at: str,
                        reason: str, operator: str) -> int:
        cur = self.conn.execute(
            "INSERT INTO overrides(scope,zone_id,action,at,reason,operator,active) "
            "VALUES(?,?,?,?,?,?,1)",
            (scope, zone_id, action, at, reason, operator),
        )
        return int(cur.lastrowid)

    def deactivate_overrides(self, scope: str, zone_id: str | None) -> None:
        if zone_id is None:
            self.conn.execute(
                "UPDATE overrides SET active=0 WHERE scope=? AND active=1", (scope,)
            )
        else:
            self.conn.execute(
                "UPDATE overrides SET active=0 WHERE scope=? AND zone_id=? AND active=1",
                (scope, zone_id),
            )

    def active_pauses(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM overrides WHERE active=1 AND action='pause'"
        ).fetchall()

    def is_paused(self, zone_id: str) -> bool:
        row = self.conn.execute(
            "SELECT COUNT(1) AS c FROM overrides WHERE active=1 AND action='pause' "
            "AND (scope='global' OR (scope='zone' AND zone_id=?))",
            (zone_id,),
        ).fetchone()
        return row["c"] > 0

    def get_zone_state(self, zone_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM zone_state WHERE zone_id=?", (zone_id,)
        ).fetchone()

    def upsert_zone_state(self, zone_id: str, **fields) -> None:
        exists = self.get_zone_state(zone_id) is not None
        if not exists:
            cols = ["zone_id", *fields.keys()]
            placeholders = ",".join("?" * len(cols))
            self.conn.execute(
                f"INSERT INTO zone_state({','.join(cols)}) VALUES({placeholders})",
                [zone_id, *fields.values()],
            )
        else:
            sets = ",".join(f"{k}=?" for k in fields)
            self.conn.execute(
                f"UPDATE zone_state SET {sets} WHERE zone_id=?",
                [*fields.values(), zone_id],
            )
