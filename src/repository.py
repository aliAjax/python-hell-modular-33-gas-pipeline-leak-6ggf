import json
import sqlite3
from datetime import datetime, timezone

from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

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
                CREATE TABLE IF NOT EXISTS valve_locks (
                    valve_id TEXT PRIMARY KEY,
                    item_id INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS work_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL UNIQUE,
                    valve_sequence TEXT NOT NULL,
                    queued_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
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

    def _apply_action_core(self, conn, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version):
        row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            raise NotFoundError("item_not_found", "业务实体不存在")
        if expected_version is not None and int(expected_version) != int(row["version"]):
            raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
        version = int(row["version"]) + 1
        conn.execute(
            "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
            (new_status, version, canonical_json(new_payload), now_iso(), item_id),
        )
        conn.execute(
            "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
            (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
        )
        self.append_audit(conn, item_id, action, actor, role, event_payload)

    def _rollback(self, conn):
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._apply_action_core(conn, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            self._rollback(conn)
            raise
        finally:
            conn.close()

    def _valve_conflicts(self, conn, item_id, sequence):
        conflicts = []
        for valve_id in sequence:
            row = conn.execute("SELECT item_id FROM valve_locks WHERE valve_id=?", (valve_id,)).fetchone()
            if row is not None and row["item_id"] != item_id:
                conflicts.append({"valve_id": valve_id, "held_by": row["item_id"]})
        return conflicts

    def _conflict_message(self, conflicts):
        held = "、".join("%s(工单#%s)" % (entry["valve_id"], entry["held_by"]) for entry in conflicts)
        return held

    def _acquire_valves(self, conn, item_id, sequence):
        for position, valve_id in enumerate(sequence):
            conn.execute(
                "INSERT INTO valve_locks(valve_id,item_id,position,created_at) VALUES(?,?,?,?)",
                (valve_id, item_id, position, now_iso()),
            )

    def _release_valves(self, conn, item_id):
        rows = conn.execute(
            "SELECT valve_id FROM valve_locks WHERE item_id=? ORDER BY position", (item_id,)
        ).fetchall()
        conn.execute("DELETE FROM valve_locks WHERE item_id=?", (item_id,))
        return [row["valve_id"] for row in rows]

    def _active_crews(self, conn):
        return conn.execute("SELECT COUNT(DISTINCT item_id) AS n FROM valve_locks").fetchone()["n"]

    def isolate_item(self, item_id, sequence, actor, role, expected_version, crew_capacity, force=False):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            payload = json.loads(row["payload"])
            sequence = list(sequence)
            if row["status"] == "isolated" and payload.get("valve_sequence") == sequence:
                conn.execute("COMMIT")
                return self.get_item(item_id)
            queued = conn.execute("SELECT valve_sequence FROM work_queue WHERE item_id=?", (item_id,)).fetchone()
            if row["status"] == "queued" and queued and json.loads(queued["valve_sequence"]) == sequence:
                conn.execute("COMMIT")
                return self.get_item(item_id)
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError("version_conflict", "记录已被其他操作更新，请重新读取")
            if row["status"] != "verified":
                raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % row["status"])
            conflicts = self._valve_conflicts(conn, item_id, sequence)
            if conflicts and force:
                raise DomainError(
                    "preempt_forbidden",
                    "属地释放前禁止抢占阀门：%s" % self._conflict_message(conflicts),
                    403,
                )
            version = int(row["version"]) + 1
            if self._active_crews(conn) >= crew_capacity:
                conn.execute(
                    "INSERT OR IGNORE INTO work_queue(item_id,valve_sequence,queued_at) VALUES(?,?,?)",
                    (item_id, canonical_json(sequence), now_iso()),
                )
                conn.execute(
                    "UPDATE items SET status=?,version=?,updated_at=? WHERE id=?",
                    ("queued", version, now_iso(), item_id),
                )
                event = {"valve_sequence": sequence, "queued": True}
                conn.execute(
                    "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, "queue", actor, role, canonical_json(event), now_iso()),
                )
                self.append_audit(conn, item_id, "queued", actor, role, event)
                conn.execute("COMMIT")
                return self.get_item(item_id)
            if conflicts:
                raise ConflictError(
                    "valve_unavailable",
                    "隔离方案不成立，以下阀门被占用：%s" % self._conflict_message(conflicts),
                )
            payload["valve_sequence"] = sequence
            payload.pop("isolation_rejection", None)
            self._acquire_valves(conn, item_id, sequence)
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                ("isolated", version, canonical_json(payload), now_iso(), item_id),
            )
            event = {"valve_sequence": sequence}
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, "isolate", actor, role, canonical_json(event), now_iso()),
            )
            self.append_audit(conn, item_id, "isolate", actor, role, event)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            self._rollback(conn)
            raise
        finally:
            conn.close()

    def _promote_queue(self, conn, crew_capacity):
        while True:
            if self._active_crews(conn) >= crew_capacity:
                return
            entry = conn.execute("SELECT * FROM work_queue ORDER BY id LIMIT 1").fetchone()
            if entry is None:
                return
            item_id = entry["item_id"]
            sequence = json.loads(entry["valve_sequence"])
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None or row["status"] != "queued":
                conn.execute("DELETE FROM work_queue WHERE id=?", (entry["id"],))
                continue
            payload = json.loads(row["payload"])
            version = int(row["version"]) + 1
            conflicts = self._valve_conflicts(conn, item_id, sequence)
            if conflicts:
                payload["isolation_rejection"] = {"blocked_valves": conflicts}
                conn.execute(
                    "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                    ("verified", version, canonical_json(payload), now_iso(), item_id),
                )
                conn.execute("DELETE FROM work_queue WHERE id=?", (entry["id"],))
                event = {"valve_sequence": sequence, "blocked_valves": conflicts}
                conn.execute(
                    "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, "isolation_rejected", "scheduler", "system", canonical_json(event), now_iso()),
                )
                self.append_audit(conn, item_id, "isolation_rejected", "scheduler", "system", event)
                continue
            payload["valve_sequence"] = sequence
            payload.pop("isolation_rejection", None)
            self._acquire_valves(conn, item_id, sequence)
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                ("isolated", version, canonical_json(payload), now_iso(), item_id),
            )
            conn.execute("DELETE FROM work_queue WHERE id=?", (entry["id"],))
            event = {"valve_sequence": sequence, "promoted": True}
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, "isolate", "scheduler", "system", canonical_json(event), now_iso()),
            )
            self.append_audit(conn, item_id, "isolate", "scheduler", "system", event)

    def restore_item(self, item_id, actor, role, new_status, new_payload, event_payload, expected_version, crew_capacity):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._apply_action_core(conn, item_id, "restore", actor, role, new_status, new_payload, event_payload, expected_version)
            released = self._release_valves(conn, item_id)
            if released:
                self.append_audit(conn, item_id, "valves_released", actor, role, {"valves": released})
            self._promote_queue(conn, crew_capacity)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            self._rollback(conn)
            raise
        finally:
            conn.close()

    def cancel_item(self, item_id, actor, role, new_status, new_payload, event_payload, expected_version):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._apply_action_core(conn, item_id, "cancel", actor, role, new_status, new_payload, event_payload, expected_version)
            conn.execute("DELETE FROM work_queue WHERE item_id=?", (item_id,))
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            self._rollback(conn)
            raise
        finally:
            conn.close()

    def valves_held(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT valve_id FROM valve_locks WHERE item_id=? ORDER BY position", (item_id,)
            ).fetchall()
            return [row["valve_id"] for row in rows]
        finally:
            conn.close()

    def queued_sequence(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT valve_sequence FROM work_queue WHERE item_id=?", (item_id,)).fetchone()
            return json.loads(row["valve_sequence"]) if row else None
        finally:
            conn.close()

    def queue_position(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT id FROM work_queue WHERE item_id=?", (item_id,)).fetchone()
            if row is None:
                return None
            ahead = conn.execute("SELECT COUNT(*) AS n FROM work_queue WHERE id<?", (row["id"],)).fetchone()["n"]
            return ahead + 1
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
            locks = [
                dict(row)
                for row in conn.execute(
                    "SELECT valve_id,item_id,position,created_at FROM valve_locks ORDER BY item_id,position"
                ).fetchall()
            ]
            queue = []
            for row in conn.execute(
                "SELECT id,item_id,valve_sequence,queued_at FROM work_queue ORDER BY id"
            ).fetchall():
                entry = dict(row)
                entry["valve_sequence"] = json.loads(entry["valve_sequence"])
                queue.append(entry)
            return {"counts": counts, "items": self.list_items(), "valve_locks": locks, "queue": queue}
        finally:
            conn.close()
