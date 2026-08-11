from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA_PATH = Path(__file__).resolve().parents[1] / "contracts" / "transaction.schema.json"


def load_schema() -> dict[str, Any]:
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


VALIDATOR = Draft202012Validator(load_schema(), format_checker=FormatChecker())


def validate_event(event: dict[str, Any]) -> list[str]:
    return sorted(error.message for error in VALIDATOR.iter_errors(event))


def idempotency_key(event: dict[str, Any]) -> str:
    canonical = json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


if __name__ == "__main__":
    sample = {
        "event_id": "evt_000001",
        "customer_id": "cust_42",
        "event_time": "2026-08-11T12:00:00Z",
        "amount": 79.5,
        "currency": "TRY",
        "merchant_id": "merchant_7",
        "country": "TR",
        "channel": "card"
    }
    print("errors:", validate_event(sample))
    print("idempotency_key:", idempotency_key(sample))
