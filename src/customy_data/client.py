from __future__ import annotations

import copy
import json
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, MutableMapping, Optional, Set, Tuple
from urllib import error, request

SDK_VERSION = "0.1.0"
CONFORMANCE_CONTRACT = "customy.customer-data-sdk.conformance.v1"
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
FORBIDDEN_TENANT_FIELDS = {
    "tenantId",
    "organizationId",
    "projectId",
    "environmentId",
}

Event = Dict[str, Any]
Transport = Callable[[str, Mapping[str, str], bytes, float], Tuple[int, bytes]]
BeforeSend = Callable[[Event], Optional[Event]]


class CustomyDataError(RuntimeError):
    def __init__(self, message: str, status_code: Optional[int] = None, response: Any = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class CustomyData:
    def __init__(
        self,
        collect_url: str,
        write_key: str,
        *,
        transport: Optional[Transport] = None,
        max_retries: int = 3,
        retry_base_seconds: float = 0.25,
        timeout_seconds: float = 10.0,
        max_batch_size: int = 100,
        max_queue_size: int = 10_000,
        redact_fields: Optional[Iterable[str]] = None,
        before_send: Optional[BeforeSend] = None,
        now: Optional[Callable[[], datetime]] = None,
        id_factory: Optional[Callable[[], str]] = None,
    ) -> None:
        if not collect_url or not write_key:
            raise ValueError("collect_url and write_key are required")
        self.collect_url = collect_url.rstrip("/")
        self.write_key = write_key
        self.transport = transport or _default_transport
        self.max_retries = max(0, max_retries)
        self.retry_base_seconds = max(0.0, retry_base_seconds)
        self.timeout_seconds = max(0.1, timeout_seconds)
        self.max_batch_size = min(1_000, max(1, max_batch_size))
        self.max_queue_size = max(1, max_queue_size)
        self.redact_fields: Set[str] = set(redact_fields or ())
        self.before_send = before_send
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.id_factory = id_factory or (lambda: str(uuid.uuid4()))
        self._queue: List[Event] = []
        self._queue_lock = threading.Lock()
        self._flush_lock = threading.Lock()

    def event(self, event: Mapping[str, Any]) -> Event:
        normalized = copy.deepcopy(dict(event))
        _reject_tenant_fields(normalized)
        if normalized.get("type") not in {"track", "identify", "group", "page", "screen", "alias"}:
            raise ValueError("type must be track, identify, group, page, screen or alias")
        if not any(normalized.get(key) for key in ("userId", "anonymousId", "groupId")):
            raise ValueError("at least one userId, anonymousId or groupId is required")
        if normalized["type"] == "track" and not normalized.get("event"):
            raise ValueError("track calls require an event name")
        normalized.setdefault("messageId", self.id_factory())
        normalized.setdefault("timestamp", _timestamp(self.now()))
        normalized.setdefault("schemaVersion", "1.0")
        normalized.setdefault("properties", {})
        normalized.setdefault("traits", {})
        normalized.setdefault("consent", {})
        context = dict(normalized.get("context") or {})
        context["library"] = {"name": "customy-data", "version": SDK_VERSION}
        normalized["context"] = context
        normalized = _redact(normalized, self.redact_fields)
        if self.before_send:
            candidate = self.before_send(copy.deepcopy(normalized))
            if candidate is None:
                raise CustomyDataError("event blocked by before_send")
            normalized = copy.deepcopy(candidate)
            _reject_tenant_fields(normalized)
            normalized = _redact(normalized, self.redact_fields)
        return normalized

    def send(self, event: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._request("event", self.event(event))

    def track(self, name: str, properties: Mapping[str, Any], **identity: Any) -> Mapping[str, Any]:
        return self.send({"type": "track", "event": name, "properties": dict(properties), **_canonical_identity(identity)})

    def identify(self, traits: Mapping[str, Any], **identity: Any) -> Mapping[str, Any]:
        return self.send({"type": "identify", "traits": dict(traits), **_canonical_identity(identity)})

    def group(self, traits: Mapping[str, Any], **identity: Any) -> Mapping[str, Any]:
        return self.send({"type": "group", "traits": dict(traits), **_canonical_identity(identity)})

    def page(self, properties: Mapping[str, Any], **identity: Any) -> Mapping[str, Any]:
        return self.send({"type": "page", "properties": dict(properties), **_canonical_identity(identity)})

    def screen(self, properties: Mapping[str, Any], **identity: Any) -> Mapping[str, Any]:
        return self.send({"type": "screen", "properties": dict(properties), **_canonical_identity(identity)})

    def alias(self, user_id: str, previous_id: str, **options: Any) -> Mapping[str, Any]:
        return self.send({
            "type": "alias",
            "userId": user_id,
            "anonymousId": previous_id,
            "properties": {"previousId": previous_id},
            **options,
        })

    def enqueue(self, event: Mapping[str, Any]) -> int:
        normalized = self.event(event)
        with self._queue_lock:
            if len(self._queue) >= self.max_queue_size:
                raise CustomyDataError("customer data queue is full")
            self._queue.append(normalized)
            return len(self._queue)

    def flush(self) -> Mapping[str, Any]:
        with self._flush_lock:
            with self._queue_lock:
                if not self._queue:
                    return {"accepted": 0, "deduplicated": 0, "quarantined": 0, "results": []}
                pending, self._queue = self._queue, []
            aggregate: Dict[str, Any] = {
                "accepted": 0,
                "deduplicated": 0,
                "quarantined": 0,
                "results": [],
            }
            try:
                for offset in range(0, len(pending), self.max_batch_size):
                    response = self._request(
                        "batch",
                        {"batch": pending[offset : offset + self.max_batch_size]},
                    )
                    for key in ("accepted", "deduplicated", "quarantined"):
                        aggregate[key] += int(response.get(key, 0))
                    aggregate["results"].extend(response.get("results", []))
                return aggregate
            except Exception:
                with self._queue_lock:
                    self._queue = pending + self._queue
                raise

    def _request(self, path: str, payload: Any) -> Mapping[str, Any]:
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        headers = {
            "content-type": "application/json",
            "user-agent": f"customy-data-python/{SDK_VERSION}",
            "x-write-key": self.write_key,
        }
        last_error: Optional[Exception] = None
        for attempt in range(self.max_retries + 1):
            try:
                status, response_body = self.transport(
                    f"{self.collect_url}/v1/collect/{path}",
                    headers,
                    body,
                    self.timeout_seconds,
                )
                parsed = _json(response_body)
                if status < 200 or status >= 300:
                    raise CustomyDataError(
                        f"Customy Data collection failed with HTTP {status}",
                        status,
                        parsed,
                    )
                return parsed if isinstance(parsed, Mapping) else {"result": parsed}
            except Exception as exc:
                last_error = exc
                if attempt >= self.max_retries or not _retryable(exc):
                    raise
                time.sleep(self.retry_base_seconds * (2**attempt))
        raise last_error or CustomyDataError("unknown collection failure")


def _default_transport(url: str, headers: Mapping[str, str], body: bytes, timeout: float) -> Tuple[int, bytes]:
    req = request.Request(url, data=body, headers=dict(headers), method="POST")
    try:
        with request.urlopen(req, timeout=timeout) as response:
            return response.status, response.read()
    except error.HTTPError as exc:
        return exc.code, exc.read()


def _timestamp(value: Any) -> str:
    if isinstance(value, str):
        return value
    if not isinstance(value, datetime):
        raise ValueError("timestamp must be a datetime or RFC3339 string")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _reject_tenant_fields(value: Mapping[str, Any]) -> None:
    found = FORBIDDEN_TENANT_FIELDS.intersection(value.keys())
    if found:
        raise ValueError(f"tenant scope is derived from the write key; forbidden fields: {sorted(found)}")


def _canonical_identity(value: Mapping[str, Any]) -> Event:
    aliases = {
        "user_id": "userId",
        "anonymous_id": "anonymousId",
        "group_id": "groupId",
    }
    return {aliases.get(key, key): item for key, item in value.items() if item is not None}


def _redact(value: Any, fields: Set[str]) -> Any:
    if isinstance(value, MutableMapping):
        return {
            key: "[REDACTED]" if key in fields else _redact(item, fields)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact(item, fields) for item in value]
    return value


def _json(value: bytes) -> Any:
    if not value:
        return {}
    try:
        return json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {"raw": value.decode("utf-8", errors="replace")}


def _retryable(exc: Exception) -> bool:
    return not isinstance(exc, CustomyDataError) or exc.status_code in RETRYABLE_STATUSES
