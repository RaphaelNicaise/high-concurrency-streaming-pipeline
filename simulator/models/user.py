import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any

class UserState(Enum):
    BROWSING_CATALOG = auto()
    VIEWING_PRODUCT = auto()
    ADDING_TO_CART = auto()
    CHECKOUT_INITIATED = auto()
    CHECKOUT_SUCCESS = auto()
    CHECKOUT_ABANDONED = auto()

_TRANSITIONS: dict[UserState, list[tuple[UserState, float]]] = {
    UserState.BROWSING_CATALOG: [
        (UserState.VIEWING_PRODUCT, 0.70),
        (UserState.BROWSING_CATALOG, 0.30),
    ],
    UserState.VIEWING_PRODUCT: [
        (UserState.ADDING_TO_CART, 0.50),
        (UserState.BROWSING_CATALOG, 0.40),
        (UserState.CHECKOUT_ABANDONED, 0.10),
    ],
    UserState.ADDING_TO_CART: [
        (UserState.CHECKOUT_INITIATED, 0.65),
        (UserState.BROWSING_CATALOG, 0.25),
        (UserState.CHECKOUT_ABANDONED, 0.10),
    ],
    UserState.CHECKOUT_INITIATED: [
        (UserState.CHECKOUT_SUCCESS, 0.70),
        (UserState.CHECKOUT_ABANDONED, 0.30),
    ],
    UserState.CHECKOUT_SUCCESS: [(UserState.BROWSING_CATALOG, 1.0)],
    UserState.CHECKOUT_ABANDONED: [(UserState.BROWSING_CATALOG, 1.0)],
}

_STATE_EVENT_MAP: dict[UserState, str] = {
    UserState.BROWSING_CATALOG: "catalog_view",
    UserState.VIEWING_PRODUCT: "product_view",
    UserState.ADDING_TO_CART: "add_to_cart",
    UserState.CHECKOUT_INITIATED: "checkout_initiated",
    UserState.CHECKOUT_SUCCESS: "purchase_completed",
    UserState.CHECKOUT_ABANDONED: "checkout_abandoned",
}

_PRODUCTS: list[tuple[str, str, float]] = [
    ("prod-001", "vip-ticket", 250.00),
    ("prod-002", "general-ticket", 80.00),
    ("prod-003", "combo-vip", 320.00),
    ("prod-004", "backstage-pass", 500.00),
    ("prod-005", "early-bird", 55.00),
    ("prod-006", "drink-combo", 35.00),
    ("prod-007", "merch-tshirt", 45.00),
    ("prod-008", "fast-lane", 90.00),
]

_DEVICE_TYPES: list[str] = [
    "mobile_ios",
    "mobile_android",
    "desktop_chrome",
    "desktop_firefox",
    "tablet_ios",
    "desktop_safari",
]

_IP_POOLS: list[str] = [
    f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}" for _ in range(500)
]

def _next_state(current: UserState) -> UserState:
    states, weights = zip(*_TRANSITIONS[current])
    return random.choices(states, weights=weights, k=1)[0]

@dataclass
class SimulatedUser:
    user_id: str = field(default_factory=lambda: f"usr-{uuid.uuid4().hex[:12]}")
    session_id: str = field(default_factory=lambda: f"sess-{uuid.uuid4().hex}")
    device_type: str = field(default_factory=lambda: random.choice(_DEVICE_TYPES))
    ip_address: str = field(default_factory=lambda: random.choice(_IP_POOLS))
    state: UserState = field(default=UserState.BROWSING_CATALOG)
    _product: tuple[str, str, float] = field(
        default_factory=lambda: random.choice(_PRODUCTS),
        repr=False,
    )

    def advance(self) -> None:
        self.state = _next_state(self.state)
        if self.state == UserState.BROWSING_CATALOG:
            self._product = random.choice(_PRODUCTS)
            self.session_id = f"sess-{uuid.uuid4().hex}"

    def build_payload(self) -> dict[str, Any]:
        product_id, category, price = self._product
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": self.user_id,
            "session_id": self.session_id,
            "event_type": _STATE_EVENT_MAP[self.state],
            "product_id": product_id,
            "category": category,
            "price": round(price * random.uniform(0.95, 1.05), 2),
            "quantity": random.randint(1, 4),
            "device_type": self.device_type,
            "ip_address": self.ip_address,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "metadata": {
                "referrer": random.choice(["google", "instagram", "direct", "email_campaign"]),
                "locale": random.choice(["es-AR", "es-MX", "pt-BR", "en-US"]),
                "app_version": f"2.{random.randint(0, 9)}.{random.randint(0, 20)}",
            },
        }
