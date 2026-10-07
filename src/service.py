from . import domain, rules
from .domain import DomainError


class Service:
    def __init__(self, repository, crew_capacity=2):
        self.repository = repository
        self.crew_capacity = max(1, int(crew_capacity))

    def create_item(self, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.CREATE_ROLES:
            raise DomainError("forbidden", "当前角色不能创建此类业务记录", 403)
        normalized = domain.normalize_create(payload)
        stable_key = normalized.pop("_stable_key")
        return self.repository.create_item(
            rules.ENTITY_TYPE, stable_key, rules.INITIAL_STATUS, normalized, actor, role
        )

    def add_source(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        item = self.repository.get_item(item_id)
        normalized = domain.normalize_source(payload)
        if region and rules.ENFORCE_REGION and role != "regulator" and normalized.get("region") and normalized["region"] != region:
            raise DomainError("region_mismatch", "来源记录不属于当前管辖区域", 403)
        result = self.repository.add_source(
            item_id,
            normalized.pop("source_type"),
            normalized.pop("external_id"),
            normalized,
            normalized.pop("observed_at"),
            actor,
            role,
        )
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            if item["payload"].get("region") != region:
                raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        if action == "isolate":
            return self._isolate(item, payload, actor, role, expected_version)
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        if action == "restore":
            self.repository.restore_item(
                item_id, actor, role, new_status, new_payload, event_payload, expected_version, self.crew_capacity
            )
        elif action == "cancel":
            self.repository.cancel_item(
                item_id, actor, role, new_status, new_payload, event_payload, expected_version
            )
        else:
            self.repository.apply_action(
                item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
            )
        return self.get_item(item_id)

    def _isolate(self, item, payload, actor, role, expected_version):
        sequence = rules.valve_sequence_from(payload)
        if item["status"] == "isolated" and item["payload"].get("valve_sequence") == sequence:
            return self.get_item(item["id"])
        if item["status"] == "queued" and self.repository.queued_sequence(item["id"]) == sequence:
            return self.get_item(item["id"])
        if item["status"] != "verified":
            raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % item["status"])
        if item["payload"].get("valve_status_conflict"):
            raise DomainError("valve_status_conflict", "阀门状态存在冲突，不能隔离", 409)
        force = bool(payload.get("force") or payload.get("preempt"))
        self.repository.isolate_item(
            item["id"], sequence, actor, role, expected_version, self.crew_capacity, force
        )
        return self.get_item(item["id"])

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        item["valves_held"] = self.repository.valves_held(item_id)
        item["queue_position"] = self.repository.queue_position(item_id)
        return item

    def list_items(self, status=None):
        return self.repository.list_items(status)

    def state(self):
        summary = self.repository.state_summary()
        summary["crew_capacity"] = self.crew_capacity
        return summary
