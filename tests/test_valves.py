import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import DomainError


class ValveResourceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        # 班组容量设为 1，便于观察“排不上先排队”。
        self.service = Service(self.repo, max_active_jobs=1)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _verified(self, idx):
        item = self.service.create_item(
            {
                "pipeline_id": "P-%d" % idx,
                "segment_id": "S-%d" % idx,
                "reported_at": "2026-10-07T0%d:00:00+00:00" % idx,
                "pressure_drop_kpa": 20 + idx,
                "sensor_value_ppm": 60,
                "odor_reports": 1,
                "reporter": "dispatch-%d" % idx,
            },
            "d-%d" % idx,
            "dispatcher",
        )
        return self.service.act(item["id"], "verify", {"field_confirmed": True}, "r", "responder", item["version"])

    def _isolate(self, item, valves):
        return self.service.act(
            item["id"], "isolate", {"valve_sequence": valves}, "s", "supervisor", item["version"]
        )

    def _restore(self, item):
        item = self.service.act(item["id"], "repair", {"work_order": "WO"}, "t", "technician", item["version"])
        item = self.service.act(
            item["id"],
            "pressure_test",
            {"test_passed": True, "pressure_kpa": 150, "minimum_pressure_kpa": 100},
            "t",
            "technician",
            item["version"],
        )
        return self.service.act(item["id"], "restore", {"hazards_clear": True}, "s", "supervisor", item["version"])

    def test_occupied_valve_rejects_plan_and_lists_conflicts(self):
        first = self._verified(1)
        first = self._isolate(first, ["V-1", "V-2"])
        self.assertEqual(first["status"], "isolated")

        second = self._verified(2)
        with self.assertRaises(DomainError) as context:
            self._isolate(second, ["V-2", "V-3"])
        err = context.exception
        self.assertEqual(err.code, "valve_occupied")
        # 方案不成立，列出被占阀门（V-2 被 first 持有），且不能各关一半。
        occupied = err.details["occupied_valves"]
        self.assertEqual([entry["valve"] for entry in occupied], ["V-2"])
        self.assertEqual(occupied[0]["held_by_item"], first["id"])
        # 第二个方案状态未变，V-3 也没有被它占位。
        second = self.service.get_item(second["id"])
        self.assertEqual(second["status"], "verified")
        state = self.service.state()
        held = {v["valve"]: v["held_by_item"] for v in state["valves"]}
        self.assertEqual(held, {"V-1": first["id"], "V-2": first["id"]})

    def test_queue_when_crew_capacity_full(self):
        first = self._verified(1)
        first = self._isolate(first, ["V-1", "V-2"])
        self.assertEqual(first["status"], "isolated")

        second = self._verified(2)
        second = self._isolate(second, ["V-3", "V-4"])
        self.assertEqual(second["status"], "queued")  # 排不上先排队，但阀门已占位

        third = self._verified(3)
        third = self._isolate(third, ["V-5", "V-6"])
        self.assertEqual(third["status"], "queued")

        state = self.service.state()
        self.assertEqual([q["item_id"] for q in state["queue"]], [second["id"], third["id"]])
        held = {v["valve"]: v["held_by_item"] for v in state["valves"]}
        self.assertEqual(held["V-3"], second["id"])
        self.assertEqual(held["V-5"], third["id"])

    def test_queued_valves_are_protected_from_preemption(self):
        first = self._verified(1)
        self._isolate(first, ["V-1", "V-1B"])
        second = self._verified(2)
        second = self._isolate(second, ["V-2", "V-2B"])  # 排队中，持有 V-2

        third = self._verified(3)
        with self.assertRaises(DomainError) as context:
            self._isolate(third, ["V-2", "V-3"])  # 属地释放前抢占 V-2
        self.assertEqual(context.exception.code, "valve_occupied")
        self.assertEqual(context.exception.details["occupied_valves"][0]["valve"], "V-2")

    def test_promotion_rejudges_valves_on_release(self):
        first = self._verified(1)
        first = self._isolate(first, ["V-1", "V-2"])
        second = self._verified(2)
        second = self._isolate(second, ["V-3", "V-4"])  # queued
        third = self._verified(3)
        third = self._isolate(third, ["V-5", "V-6"])  # queued

        # 释放第一个方案 -> 队首 second 按当时阀门状态重新判断并晋升。
        first = self._restore(first)
        self.assertEqual(first["status"], "restored")
        second = self.service.get_item(second["id"])
        self.assertEqual(second["status"], "isolated")
        third = self.service.get_item(third["id"])
        self.assertEqual(third["status"], "queued")
        self.assertEqual([q["item_id"] for q in self.service.state()["queue"]], [third["id"]])

        # 释放第二个方案 -> third 晋升。
        second = self._restore(second)
        self.assertEqual(second["status"], "restored")
        third = self.service.get_item(third["id"])
        self.assertEqual(third["status"], "isolated")
        self.assertEqual(self.service.state()["queue"], [])

    def test_cancel_releases_and_promotes(self):
        first = self._verified(1)
        first = self._isolate(first, ["V-1", "V-1B"])
        second = self._verified(2)
        second = self._isolate(second, ["V-2", "V-2B"])  # queued

        first = self.service.act(first["id"], "cancel", {"reason": "plan changed"}, "s", "supervisor", first["version"])
        self.assertEqual(first["status"], "cancelled")
        second = self.service.get_item(second["id"])
        self.assertEqual(second["status"], "isolated")  # 释放后队首晋升
        held = {v["valve"]: v["held_by_item"] for v in self.service.state()["valves"]}
        self.assertEqual(held, {"V-2": second["id"], "V-2B": second["id"]})

    def test_duplicate_submission_does_not_double_occupy(self):
        first = self._verified(1)
        first = self._isolate(first, ["V-1", "V-2"])
        # 重复提交隔离（重试）：状态已不是 verified，不能再次占位。
        with self.assertRaises(DomainError) as context:
            self._isolate(first, ["V-1", "V-2"])
        self.assertEqual(context.exception.code, "invalid_state")
        state = self.service.state()
        held = {v["valve"]: v["held_by_item"] for v in state["valves"]}
        self.assertEqual(held, {"V-1": first["id"], "V-2": first["id"]})
        self.assertEqual(len(state["valves"]), 2)

    def test_occupations_and_queue_survive_restart(self):
        first = self._verified(1)
        first = self._isolate(first, ["V-1", "V-2"])
        second = self._verified(2)
        second = self._isolate(second, ["V-3", "V-4"])  # queued

        # 用同一个数据库文件重新构造，模拟服务重启。
        restarted_repo = Repository(self.tmp.name)
        restarted = Service(restarted_repo, max_active_jobs=1)
        state = restarted.state()
        held = {v["valve"]: v["held_by_item"] for v in state["valves"]}
        self.assertEqual(held, {"V-1": first["id"], "V-2": first["id"], "V-3": second["id"], "V-4": second["id"]})
        self.assertEqual([q["item_id"] for q in state["queue"]], [second["id"]])
        # 重启后释放 first，仍能按 FIFO 晋升 second。
        first = restarted.get_item(first["id"])
        first = restarted.act(first["id"], "repair", {"work_order": "WO"}, "t", "technician", first["version"])
        first = restarted.act(
            first["id"],
            "pressure_test",
            {"test_passed": True, "pressure_kpa": 150, "minimum_pressure_kpa": 100},
            "t",
            "technician",
            first["version"],
        )
        first = restarted.act(first["id"], "restore", {"hazards_clear": True}, "s", "supervisor", first["version"])
        self.assertEqual(first["status"], "restored")
        second = restarted.get_item(second["id"])
        self.assertEqual(second["status"], "isolated")


if __name__ == "__main__":
    unittest.main()
