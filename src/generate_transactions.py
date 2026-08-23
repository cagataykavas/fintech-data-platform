from __future__ import annotations

import argparse
import json
import random
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class TransactionEvent:
    event_id: str
    customer_id: str
    event_time: str
    amount: float
    currency: str
    merchant_id: str
    country: str
    channel: str


COUNTRIES = ("TR", "DE", "NL", "GB", "US", "JP")
CHANNELS = ("card", "bank_transfer", "wallet", "atm")
CURRENCIES = ("TRY", "EUR", "USD", "GBP")


def generate_events(count: int, seed: int = 42, start: datetime | None = None) -> list[TransactionEvent]:
    rng = random.Random(seed)
    current = start or datetime(2026, 8, 1, tzinfo=timezone.utc)
    events: list[TransactionEvent] = []
    for index in range(count):
        current += timedelta(seconds=rng.randint(1, 90))
        channel = rng.choices(CHANNELS, weights=(0.62, 0.16, 0.17, 0.05), k=1)[0]
        currency = rng.choices(CURRENCIES, weights=(0.55, 0.20, 0.20, 0.05), k=1)[0]
        baseline = {"card": 95, "bank_transfer": 850, "wallet": 55, "atm": 280}[channel]
        amount = max(1.0, rng.lognormvariate(0.0, 0.9) * baseline)
        events.append(
            TransactionEvent(
                event_id=f"evt-{index:08d}-{uuid.UUID(int=rng.getrandbits(128))}",
                customer_id=f"cust-{rng.randint(1, max(20, count // 25)):06d}",
                event_time=current.isoformat().replace("+00:00", "Z"),
                amount=round(amount, 2),
                currency=currency,
                merchant_id=f"merchant-{rng.randint(1, 250):04d}",
                country=rng.choices(COUNTRIES, weights=(0.66, 0.09, 0.06, 0.07, 0.08, 0.04), k=1)[0],
                channel=channel,
            )
        )
    return events


def write_jsonl(events: list[TransactionEvent], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for event in events:
            handle.write(json.dumps(asdict(event), separators=(",", ":")) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic synthetic financial transaction events.")
    parser.add_argument("--count", type=int, default=10000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output", type=Path, default=Path("data/incoming/transactions.jsonl"))
    args = parser.parse_args()
    events = generate_events(args.count, args.seed)
    write_jsonl(events, args.output)
    print(f"wrote {len(events)} synthetic events to {args.output}")


if __name__ == "__main__":
    main()
