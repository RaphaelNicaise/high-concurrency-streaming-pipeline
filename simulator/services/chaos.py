import json
import logging
import random
import string
from datetime import datetime, timedelta, timezone
from typing import Any

class ChaosInjector:
    _STRATEGIES: list[str] = [
        "null_required_field",
        "negative_id",
        "string_in_numeric_field",
        "corrupted_timestamp",
        "future_timestamp",
        "missing_keys",
        "broken_json_fragment",
    ]

    def __init__(self, chaos_ratio: float, logger: logging.Logger) -> None:
        if not 0.0 <= chaos_ratio <= 1.0:
            raise ValueError(f"chaos_ratio must be in [0.0, 1.0], got {chaos_ratio}")
        self.chaos_ratio = chaos_ratio
        self._log = logger.getChild("chaos")

    def should_inject(self) -> bool:
        return random.random() < self.chaos_ratio

    def inject(self, payload: dict[str, Any]) -> "dict[str, Any] | str":
        strategy = random.choice(self._STRATEGIES)
        self._log.debug("Injecting chaos: strategy=%s", strategy)
        corrupted = dict(payload)

        if strategy == "null_required_field":
            field = random.choice(["event_type", "user_id", "session_id"])
            corrupted[field] = None

        elif strategy == "negative_id":
            corrupted["user_id"] = -random.randint(1, 99_999)

        elif strategy == "string_in_numeric_field":
            corrupted["price"] = "NaN_" + "".join(
                random.choices(string.ascii_uppercase, k=6)
            )

        elif strategy == "corrupted_timestamp":
            corrupted["timestamp"] = (
                datetime.now(tz=timezone.utc) - timedelta(days=3_650)
            ).isoformat()

        elif strategy == "future_timestamp":
            corrupted["timestamp"] = (
                datetime.now(tz=timezone.utc) + timedelta(days=1_825)
            ).isoformat()

        elif strategy == "missing_keys":
            keys_to_drop = random.sample(
                list(corrupted.keys()), k=min(3, len(corrupted))
            )
            for k in keys_to_drop:
                corrupted.pop(k, None)

        elif strategy == "broken_json_fragment":
            raw = json.dumps(corrupted)
            cut = random.randint(10, min(40, len(raw)))
            return raw[:cut] + "<<CORRUPTED_PAYLOAD>>"

        return corrupted
