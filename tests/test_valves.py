import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError


class ValveResourceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo, crew_capacity=2)
        self.seq = 0

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _verified(self, service=None):
        service = service or self.service
        self.seq += 1
        item = service.create_item({
            "pipeline_id": "P-1",
            "segment_id": "S-%d" % self.seq,
            "reported_at": "2026-09-27T08:00:00+00:00",
            "pressure_drop_kpa": 30,
            "sensor_value_ppm": 120,
            "odor_reports": 1,
            "reporter": "dispatch-1",
        }, "dispatch-1", "dispatcher")
        return service.act(item["id"], "verify", {"field_confirmed": True}, "resp-1", "responder", item["version"])

    def _isolate(self, item, sequence, service=None, **extra):
        service = service or self.service
        payload = {"valve_sequence": sequence}
        payload.update(extra)
        return service.act(item["id"], "isolate", payload, "sup-1", "supervisor", item["version"])

    def _finish_restore(self, item, service=None):
        service = service or self.service
        item = service.act(item["id"], "repair", {"work_order": "WO-1"}, "tech-1", "technician", item["version"])
        item = service.act(item["id"], "pressure_test", {"test_passed": True, "pressure_kpa": 150, "minimum_pressure_kpa": 100}, "tech-1", "technician", item["version"])
        return service.act(item["id"], "restore", {"hazards_clear": True}, "sup-1", "supervisor", item["version"])

    def test_valve_conflict_lists_held_valves(self):
        a = self._isolate(self._verified(), ["V-1", "V-2"])
        self.assertEqual(a["status"], "isolated")
        self.assertEqual(a["valves_held"], ["V-1", "V-2"])
        b = self._verified()
        with self.assertRaises(ConflictError) as context:
            self._isolate(b, ["V-2", "V-3"])
        self.assertEqual(context.exception.code, "valve_unavailable")
        self.assertIn("V-2", str(context.exception))
        self.assertIn(str(a["id"]), str(context.exception))
        self.assertEqual(self.service.get_item(b["id"])["status"], "verified")

    def test_preemption_rejected_before_release(self):
        a = self._isolate(self._verified(), ["V-1", "V-2"])
        b = self._verified()
        with self.assertRaises(DomainError) as context:
            self._isolate(b, ["V-2", "V-3"], force=True)
        self.assertEqual(context.exception.code, "preempt_forbidden")
        self.assertEqual(context.exception.status, 403)
        self._finish_restore(a)
        b = self._isolate(b, ["V-2", "V-3"])
        self.assertEqual(b["status"], "isolated")
        self.assertEqual(b["valves_held"], ["V-2", "V-3"])

    def test_queue_then_promote_in_submission_order(self):
        self.service.crew_capacity = 1
        a = self._isolate(self._verified(), ["V-1", "V-2"])
        b = self._isolate(self._verified(), ["V-3", "V-4"])
        self.assertEqual(b["status"], "queued")
        self.assertEqual(b["queue_position"], 1)
        c = self._isolate(self._verified(), ["V-5", "V-6"])
        self.assertEqual(c["status"], "queued")
        self.assertEqual(c["queue_position"], 2)
        self._finish_restore(a)
        b = self.service.get_item(b["id"])
        self.assertEqual(b["status"], "isolated")
        self.assertEqual(b["valves_held"], ["V-3", "V-4"])
        self.assertIsNone(b["queue_position"])
        self.assertEqual(self.service.get_item(c["id"])["queue_position"], 1)

    def test_queued_plan_rejected_if_valves_still_held(self):
        a = self._isolate(self._verified(), ["V-1", "V-2"])
        b = self._isolate(self._verified(), ["V-3", "V-4"])
        c = self._isolate(self._verified(), ["V-3", "V-5"])
        self.assertEqual(c["status"], "queued")
        d = self._isolate(self._verified(), ["V-6", "V-7"])
        self.assertEqual(d["status"], "queued")
        self._finish_restore(a)
        c = self.service.get_item(c["id"])
        self.assertEqual(c["status"], "verified")
        blocked = c["payload"]["isolation_rejection"]["blocked_valves"]
        self.assertEqual(blocked, [{"valve_id": "V-3", "held_by": b["id"]}])
        d = self.service.get_item(d["id"])
        self.assertEqual(d["status"], "isolated")
        self.assertEqual(d["valves_held"], ["V-6", "V-7"])

    def test_duplicate_submission_does_not_double_occupy(self):
        self.service.crew_capacity = 1
        a = self._isolate(self._verified(), ["V-1", "V-2"])
        again = self._isolate(a, ["V-1", "V-2"])
        self.assertEqual(again["status"], "isolated")
        locks = [entry for entry in self.service.state()["valve_locks"] if entry["item_id"] == a["id"]]
        self.assertEqual(len(locks), 2)
        b = self._isolate(self._verified(), ["V-3", "V-4"])
        self.assertEqual(b["status"], "queued")
        again = self._isolate(b, ["V-3", "V-4"])
        self.assertEqual(again["status"], "queued")
        self.assertEqual(len(self.service.state()["queue"]), 1)

    def test_restart_reads_back_locks_and_queue(self):
        self.service.crew_capacity = 1
        a = self._isolate(self._verified(), ["V-1", "V-2"])
        b = self._isolate(self._verified(), ["V-3", "V-4"])
        self.assertEqual(b["status"], "queued")
        service2 = Service(Repository(self.tmp.name), crew_capacity=1)
        state = service2.state()
        locks = {entry["valve_id"]: entry["item_id"] for entry in state["valve_locks"]}
        self.assertEqual(locks, {"V-1": a["id"], "V-2": a["id"]})
        self.assertEqual([entry["item_id"] for entry in state["queue"]], [b["id"]])
        c = self._verified(Service(Repository(self.tmp.name), crew_capacity=5))
        with self.assertRaises(ConflictError):
            self._isolate(c, ["V-1", "V-9"], Service(Repository(self.tmp.name), crew_capacity=5))
        self._finish_restore(service2.get_item(a["id"]), service2)
        b = service2.get_item(b["id"])
        self.assertEqual(b["status"], "isolated")
        self.assertEqual(b["valves_held"], ["V-3", "V-4"])

    def test_cancel_queued_item_leaves_queue(self):
        self.service.crew_capacity = 1
        a = self._isolate(self._verified(), ["V-1", "V-2"])
        b = self._isolate(self._verified(), ["V-3", "V-4"])
        self.assertEqual(b["status"], "queued")
        b = self.service.act(b["id"], "cancel", {"reason": "误报"}, "sup-1", "supervisor", b["version"])
        self.assertEqual(b["status"], "cancelled")
        self.assertEqual(self.service.state()["queue"], [])
        self._finish_restore(a)
        self.assertEqual(self.service.state()["queue"], [])

    def test_concurrent_submissions_do_not_split_valves(self):
        self.service.crew_capacity = 5
        first = self._verified()
        second = self._verified()
        results = []

        def submit(item):
            try:
                results.append(("ok", self._isolate(item, ["V-8", "V-9"])))
            except DomainError as exc:
                results.append(("error", exc))

        threads = [threading.Thread(target=submit, args=(item,)) for item in (first, second)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        outcomes = sorted(kind for kind, _ in results)
        self.assertEqual(outcomes, ["error", "ok"])
        error = [value for kind, value in results if kind == "error"][0]
        self.assertEqual(error.code, "valve_unavailable")
        locks = [entry for entry in self.service.state()["valve_locks"] if entry["valve_id"] == "V-8"]
        self.assertEqual(len(locks), 1)

    def test_duplicate_valve_in_sequence_rejected(self):
        item = self._verified()
        with self.assertRaises(DomainError) as context:
            self._isolate(item, ["V-1", "V-1"])
        self.assertEqual(context.exception.code, "duplicate_valve")


if __name__ == "__main__":
    unittest.main()
