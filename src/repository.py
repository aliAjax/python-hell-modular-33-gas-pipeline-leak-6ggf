import json
import sqlite3
from datetime import datetime, timezone

from . import rules
from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path, max_active_jobs=None):
        self.path = path
        # 同时在场隔离抢修的方案上限；占得到阀门但排不上班组的方案进入排队。
        self.max_active_jobs = max_active_jobs if max_active_jobs is not None else rules.MAX_ACTIVE_JOBS

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self):
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    stable_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, stable_key)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    role TEXT,
                    payload TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS valves (
                    valve_name TEXT PRIMARY KEY,
                    held_by_item INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY(held_by_item) REFERENCES items(id)
                );
                """
            )
        finally:
            conn.close()

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _last_hash(self, conn, item_id):
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE item_id IS ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        return row["event_hash"] if row else "GENESIS"

    def append_audit(self, conn, item_id, event_type, actor, role, payload):
        previous = self._last_hash(conn, item_id)
        event = {
            "item_id": item_id,
            "event_type": event_type,
            "actor": actor,
            "role": role,
            "payload": payload,
            "created_at": now_iso(),
        }
        event_hash = audit_hash(previous, event)
        conn.execute(
            "INSERT INTO audit_events(item_id,event_type,actor,role,payload,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (item_id, event_type, actor, role, canonical_json(payload), previous, event_hash, event["created_at"]),
        )

    def _occupy_valves(self, conn, item_id, valves):
        """按上游到下游顺序占位，原子操作。

        任一阀门已被其他方案持有则抛 valve_occupied（不写任何阀门），由外层回滚。
        一个阀门同一时间只归一个隔离方案；属地释放前抢占他人阀门一律拒绝。
        """
        holders = {}
        if valves:
            placeholders = ",".join("?" for _ in valves)
            rows = conn.execute(
                "SELECT valve_name, held_by_item FROM valves WHERE valve_name IN (%s)" % placeholders,
                list(valves),
            ).fetchall()
            for row in rows:
                holders[row["valve_name"]] = row["held_by_item"]
        conflicts = []
        for name in valves:  # 严格按上游→下游顺序核对并列出冲突
            holder = holders.get(name)
            if holder is not None and int(holder) != int(item_id):
                conflicts.append({"valve": name, "held_by_item": holder})
        if conflicts:
            raise DomainError(
                "valve_occupied",
                "隔离方案不成立：阀门已被其他隔离方案占用",
                409,
                {"occupied_valves": conflicts},
            )
        ts = now_iso()
        for name in valves:
            conn.execute(
                "INSERT OR REPLACE INTO valves(valve_name,held_by_item,updated_at) VALUES(?,?,?)",
                (name, item_id, ts),
            )

    def _release_valves(self, conn, item_id):
        conn.execute("DELETE FROM valves WHERE held_by_item=?", (item_id,))

    def _active_count(self, conn):
        row = conn.execute("SELECT COUNT(*) AS total FROM items WHERE status='isolated'").fetchone()
        return row["total"]

    def _promote_next(self, conn):
        """释放一个班组名额后，按 FIFO 晋升队首方案；晋升前按当时阀门状态重新判断。

        队首方案的阀门若仍被本方案持有则直接晋升；若被其他方案抢占（异常），
        则保留排队、不跳号（不抢占后面方案的顺位）。
        """
        if self._active_count(conn) >= self.max_active_jobs:
            return
        row = conn.execute(
            "SELECT id, version, payload FROM items WHERE status='queued' ORDER BY id LIMIT 1"
        ).fetchone()
        if row is None:
            return
        item_id = row["id"]
        payload = json.loads(row["payload"])
        valves = payload.get("valve_sequence") or []
        try:
            self._occupy_valves(conn, item_id, valves)
        except DomainError:
            return  # 队首阀门被占，本轮不晋升；顺位保留，等下一次释放再判断
        ts = now_iso()
        cur = conn.execute(
            "UPDATE items SET status='isolated', version=?, updated_at=? WHERE id=? AND status='queued'",
            (int(row["version"]) + 1, ts, item_id),
        )
        if cur.rowcount != 1:
            return  # 队首状态已变，未发生晋升，不记审计
        conn.execute(
            "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
            (item_id, "schedule", "system", "scheduler", canonical_json({"valve_sequence": valves}), ts),
        )
        self.append_audit(conn, item_id, "scheduled", "system", "scheduler", {"valve_sequence": valves})

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        entity_type,
                        stable_key,
                        initial_status,
                        1,
                        canonical_json(payload),
                        actor,
                        role,
                        now_iso(),
                        now_iso(),
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_item", "同一业务实体已经存在")
            item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def list_items(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM items WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM items ORDER BY id DESC").fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_source", "同一来源记录已经提交")
            source_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM sources WHERE item_id=? ORDER BY id DESC", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            version = int(row["version"]) + 1

            if action == "isolate":
                # 原子占位：按上游→下游逐个核对，任一被占即回滚并列出被占阀门（方案不成立）。
                valves = new_payload.get("valve_sequence") or []
                self._occupy_valves(conn, item_id, valves)
                # 占得到阀门后再看班组容量：排不上就进排队，顺位按提交先后。
                if self._active_count(conn) >= self.max_active_jobs:
                    new_status = "queued"
                else:
                    new_status = "isolated"

            # 先更新本方案状态，释放类操作再据此归还阀门、晋升排队（此时名额才真正腾出）。
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )

            if action in ("restore", "cancel"):
                # 属地释放：归还本方案占用的阀门，再按 FIFO 晋升排队方案。
                self._release_valves(conn, item_id)
                self._promote_next(conn)

            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
            )
            self.append_audit(conn, item_id, action, actor, role, event_payload)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def audit_trail(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM audit_events WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            valves = []
            for row in conn.execute(
                "SELECT v.valve_name, v.held_by_item, v.updated_at, i.payload, i.status "
                "FROM valves v JOIN items i ON i.id = v.held_by_item ORDER BY v.valve_name"
            ).fetchall():
                payload = json.loads(row["payload"])
                valves.append(
                    {
                        "valve": row["valve_name"],
                        "held_by_item": row["held_by_item"],
                        "segment_id": payload.get("segment_id"),
                        "status": row["status"],
                        "updated_at": row["updated_at"],
                    }
                )
            queue = []
            for row in conn.execute(
                "SELECT id, payload, status, updated_at FROM items "
                "WHERE status='queued' ORDER BY id"
            ).fetchall():
                payload = json.loads(row["payload"])
                queue.append(
                    {
                        "item_id": row["id"],
                        "segment_id": payload.get("segment_id"),
                        "status": row["status"],
                        "enqueued_at": row["updated_at"],
                    }
                )
            return {
                "counts": counts,
                "items": self.list_items(),
                "valves": valves,
                "queue": queue,
                "max_active_jobs": self.max_active_jobs,
            }
        finally:
            conn.close()
