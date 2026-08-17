import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from customy_data import CONFORMANCE_CONTRACT, CustomyData, CustomyDataError  # noqa: E402


class RecordingTransport:
    def __init__(self, statuses=None):
        self.statuses = list(statuses or [202])
        self.calls = []

    def __call__(self, url, headers, body, timeout):
        payload = json.loads(body)
        self.calls.append((url, dict(headers), payload, timeout))
        status = self.statuses.pop(0) if self.statuses else 202
        response = (
            {"accepted": len(payload.get("batch", [payload])), "deduplicated": 0, "quarantined": 0, "results": []}
            if status < 300
            else {"error": "temporary"}
        )
        return status, json.dumps(response).encode()


class CustomyDataTests(unittest.TestCase):
    def client(self, transport, **options):
        counter = iter(range(1, 100))
        return CustomyData(
            "https://data.customy.ai/",
            "cdw_test",
            transport=transport,
            retry_base_seconds=0,
            now=lambda: datetime(2026, 8, 16, tzinfo=timezone.utc),
            id_factory=lambda: f"message_{next(counter)}",
            **options,
        )

    def test_portable_six_call_conformance(self):
        vectors = json.loads((ROOT / "conformance" / "customer-data-v1.json").read_text())
        self.assertEqual(vectors["contract"], CONFORMANCE_CONTRACT)
        transport = RecordingTransport([202] * len(vectors["eventTypes"]))
        client = self.client(transport)
        for event in vectors["eventTypes"]:
            client.send(event)
        self.assertEqual([call[2]["type"] for call in transport.calls], ["track", "identify", "group", "page", "screen", "alias"])
        for _, headers, event, _ in transport.calls:
            self.assertEqual(headers["x-write-key"], "cdw_test")
            for key in vectors["forbiddenPayloadKeys"]:
                self.assertNotIn(key, event)
            self.assertEqual(event["schemaVersion"], "1.0")
            self.assertEqual(event["context"]["library"]["name"], "customy-data")
            self.assertEqual(event["consent"], {})

    def test_retries_without_changing_message_id(self):
        transport = RecordingTransport([503, 429, 202])
        client = self.client(transport)
        client.track("Checkout Started", {"value": 10}, anonymous_id="anon_1")
        self.assertEqual(len(transport.calls), 3)
        self.assertEqual({call[2]["messageId"] for call in transport.calls}, {"message_1"})

    def test_redacts_before_network_and_rejects_tenant_scope(self):
        transport = RecordingTransport()
        client = self.client(transport, redact_fields={"password", "cardNumber"})
        client.identify(
            {"email": "buyer@example.com", "password": "secret", "payment": {"cardNumber": "4111"}},
            user_id="user_1",
        )
        traits = transport.calls[0][2]["traits"]
        self.assertEqual(traits["password"], "[REDACTED]")
        self.assertEqual(traits["payment"]["cardNumber"], "[REDACTED]")
        with self.assertRaises(ValueError):
            client.send({"type": "identify", "userId": "u", "organizationId": "forged"})

    def test_batch_chunks_and_restores_queue_after_partial_failure(self):
        transport = RecordingTransport([202, 503])
        client = self.client(transport, max_batch_size=2, max_retries=0)
        for name in ("A", "B", "C"):
            client.enqueue({"type": "track", "event": name, "anonymousId": "anon_1"})
        with self.assertRaises(CustomyDataError):
            client.flush()
        self.assertEqual(client.enqueue({"type": "track", "event": "D", "anonymousId": "anon_1"}), 4)

    def test_before_send_can_block_collection(self):
        transport = RecordingTransport()
        client = self.client(transport, before_send=lambda event: None)
        with self.assertRaises(CustomyDataError):
            client.send({"type": "track", "event": "Blocked", "anonymousId": "anon_1"})
        self.assertEqual(transport.calls, [])

    def test_before_send_cannot_reintroduce_pii(self):
        transport = RecordingTransport()
        client = self.client(
            transport,
            redact_fields={"password"},
            before_send=lambda event: {**event, "traits": {"password": "reintroduced"}},
        )
        client.identify({}, user_id="user_1")
        self.assertEqual(transport.calls[0][2]["traits"]["password"], "[REDACTED]")


if __name__ == "__main__":
    unittest.main()
