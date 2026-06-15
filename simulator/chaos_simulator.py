"""
chaos_simulator.py — TapDrink High-Concurrency Telemetry & Chaos Simulator
===========================================================================
Simulates realistic e-commerce/ticketing traffic against the ingest API,
including flash-sale spikes and intentional data-quality failures.

Architecture:
  - asyncio + aiohttp for non-blocking concurrent HTTP
  - SimulatedUser state machine per virtual user
  - ChaosInjector for controlled bad data
  - TrafficController for dynamic ramp-up / flash-sale spikes
  - MetricsCollector for live throughput reporting
  - Config via environment variables

Usage:
  python chaos_simulator.py

Environment Variables (see Config class for defaults):
  INGEST_API_URL        Base URL of the FastAPI ingest endpoint
  BASE_WORKERS          Number of concurrent virtual users during normal traffic
  SPIKE_MULTIPLIER      How many times to multiply workers during a flash sale
  SPIKE_DURATION_S      Duration of each traffic spike in seconds
  SPIKE_INTERVAL_S      Seconds between spikes
  CHAOS_RATIO           Fraction of events that are intentionally malformed (0.0-1.0)
  REQUEST_TIMEOUT_S     Per-request aiohttp timeout in seconds
  REPORT_INTERVAL_S     How often (seconds) to print the live metrics report
  MAX_RETRIES           Number of HTTP retries on transient errors
  LOG_LEVEL             Python logging level (DEBUG, INFO, WARNING, ERROR)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import string
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum, auto
from typing import Any

import aiohttp

# -----------------------------------------------------------------------------
# Logging Setup
# -----------------------------------------------------------------------------

def _setup_logging(level: str) -> logging.Logger:
    """Configure a structured console logger."""
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    logger = logging.getLogger("chaos_simulator")
    logger.setLevel(numeric_level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class Config:
    """
    All runtime configuration loaded from environment variables.
    Every field has a sensible default for local development.
    """
    ingest_api_url: str      = field(default_factory=lambda: os.getenv("INGEST_API_URL", "http://localhost:8000"))
    base_workers: int        = field(default_factory=lambda: int(os.getenv("BASE_WORKERS", "50")))
    spike_multiplier: int    = field(default_factory=lambda: int(os.getenv("SPIKE_MULTIPLIER", "10")))
    spike_duration_s: float  = field(default_factory=lambda: float(os.getenv("SPIKE_DURATION_S", "30")))
    spike_interval_s: float  = field(default_factory=lambda: float(os.getenv("SPIKE_INTERVAL_S", "120")))
    chaos_ratio: float       = field(default_factory=lambda: float(os.getenv("CHAOS_RATIO", "0.05")))
    request_timeout_s: float = field(default_factory=lambda: float(os.getenv("REQUEST_TIMEOUT_S", "10")))
    report_interval_s: float = field(default_factory=lambda: float(os.getenv("REPORT_INTERVAL_S", "5")))
    max_retries: int         = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    log_level: str           = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    @property
    def ingest_endpoint(self) -> str:
        return f"{self.ingest_api_url.rstrip('/')}/events"

    @property
    def spike_workers(self) -> int:
        return self.base_workers * self.spike_multiplier


# -----------------------------------------------------------------------------
# User State Machine
# -----------------------------------------------------------------------------

class UserState(Enum):
    BROWSING_CATALOG   = auto()
    VIEWING_PRODUCT    = auto()
    ADDING_TO_CART     = auto()
    CHECKOUT_INITIATED = auto()
    CHECKOUT_SUCCESS   = auto()
    CHECKOUT_ABANDONED = auto()


# Allowed state transitions: state -> list of (next_state, weight)
_TRANSITIONS: dict[UserState, list[tuple[UserState, float]]] = {
    UserState.BROWSING_CATALOG:   [
        (UserState.VIEWING_PRODUCT, 0.70),
        (UserState.BROWSING_CATALOG, 0.30),
    ],
    UserState.VIEWING_PRODUCT:    [
        (UserState.ADDING_TO_CART, 0.50),
        (UserState.BROWSING_CATALOG, 0.40),
        (UserState.CHECKOUT_ABANDONED, 0.10),
    ],
    UserState.ADDING_TO_CART:     [
        (UserState.CHECKOUT_INITIATED, 0.65),
        (UserState.BROWSING_CATALOG, 0.25),
        (UserState.CHECKOUT_ABANDONED, 0.10),
    ],
    UserState.CHECKOUT_INITIATED: [
        (UserState.CHECKOUT_SUCCESS, 0.70),
        (UserState.CHECKOUT_ABANDONED, 0.30),
    ],
    # Terminal states loop back to a fresh session
    UserState.CHECKOUT_SUCCESS:   [(UserState.BROWSING_CATALOG, 1.0)],
    UserState.CHECKOUT_ABANDONED: [(UserState.BROWSING_CATALOG, 1.0)],
}

# Map state -> event_type string sent in the payload
_STATE_EVENT_MAP: dict[UserState, str] = {
    UserState.BROWSING_CATALOG:   "catalog_view",
    UserState.VIEWING_PRODUCT:    "product_view",
    UserState.ADDING_TO_CART:     "add_to_cart",
    UserState.CHECKOUT_INITIATED: "checkout_initiated",
    UserState.CHECKOUT_SUCCESS:   "purchase_completed",
    UserState.CHECKOUT_ABANDONED: "checkout_abandoned",
}

# Realistic catalogue: (product_id, category, price)
_PRODUCTS: list[tuple[str, str, float]] = [
    ("prod-001", "vip-ticket",     250.00),
    ("prod-002", "general-ticket",  80.00),
    ("prod-003", "combo-vip",      320.00),
    ("prod-004", "backstage-pass", 500.00),
    ("prod-005", "early-bird",      55.00),
    ("prod-006", "drink-combo",     35.00),
    ("prod-007", "merch-tshirt",    45.00),
    ("prod-008", "fast-lane",       90.00),
]

_DEVICE_TYPES: list[str] = [
    "mobile_ios", "mobile_android", "desktop_chrome",
    "desktop_firefox", "tablet_ios", "desktop_safari",
]

# Pre-generate a pool of realistic-looking IPs for diversity
_IP_POOLS: list[str] = [
    f"192.168.{random.randint(0, 255)}.{random.randint(1, 254)}"
    for _ in range(500)
]


def _next_state(current: UserState) -> UserState:
    """Transition to the next state using weighted random selection."""
    states, weights = zip(*_TRANSITIONS[current])
    return random.choices(states, weights=weights, k=1)[0]  # type: ignore[return-value]


@dataclass
class SimulatedUser:
    """
    Represents a single virtual user navigating TapDrink.
    Maintains its own session, device fingerprint, and current state.
    Advances through a realistic e-commerce journey via a state machine.
    """
    user_id: str     = field(default_factory=lambda: f"usr-{uuid.uuid4().hex[:12]}")
    session_id: str  = field(default_factory=lambda: f"sess-{uuid.uuid4().hex}")
    device_type: str = field(default_factory=lambda: random.choice(_DEVICE_TYPES))
    ip_address: str  = field(default_factory=lambda: random.choice(_IP_POOLS))
    state: UserState = field(default=UserState.BROWSING_CATALOG)
    _product: tuple[str, str, float] = field(
        default_factory=lambda: random.choice(_PRODUCTS),
        repr=False,
    )

    def advance(self) -> None:
        """Move to the next logical state in the user journey."""
        self.state = _next_state(self.state)
        if self.state == UserState.BROWSING_CATALOG:
            # New session = new product interest and fresh session ID
            self._product = random.choice(_PRODUCTS)
            self.session_id = f"sess-{uuid.uuid4().hex}"

    def build_payload(self) -> dict[str, Any]:
        """
        Construct a realistic, fully-typed event payload for the current state.
        All fields reflect what a real frontend SDK would send.
        """
        product_id, category, price = self._product
        return {
            "event_id":    str(uuid.uuid4()),
            "user_id":     self.user_id,
            "session_id":  self.session_id,
            "event_type":  _STATE_EVENT_MAP[self.state],
            "product_id":  product_id,
            "category":    category,
            "price":       round(price * random.uniform(0.95, 1.05), 2),
            "quantity":    random.randint(1, 4),
            "device_type": self.device_type,
            "ip_address":  self.ip_address,
            "timestamp":   datetime.now(tz=timezone.utc).isoformat(),
            "metadata": {
                "referrer":    random.choice(["google", "instagram", "direct", "email_campaign"]),
                "locale":      random.choice(["es-AR", "es-MX", "pt-BR", "en-US"]),
                "app_version": f"2.{random.randint(0, 9)}.{random.randint(0, 20)}",
            },
        }


# -----------------------------------------------------------------------------
# Chaos Injector
# -----------------------------------------------------------------------------

class ChaosInjector:
    """
    Responsible for intentionally corrupting a fraction of event payloads
    to stress-test the Data Quality layer (Great Expectations / Spark).

    Corruption strategies:
      - null_required_field     : sets event_type / user_id / session_id to None
      - negative_id             : replaces user_id with a negative integer
      - string_in_numeric_field : injects a string where price should be
      - corrupted_timestamp     : timestamp 10 years in the past
      - future_timestamp        : timestamp 5 years in the future
      - missing_keys            : drops 3 random required fields
      - broken_json_fragment    : truncates JSON mid-way and appends garbage
    """

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
        """Return True if this event should be corrupted based on the ratio."""
        return random.random() < self.chaos_ratio

    def inject(self, payload: dict[str, Any]) -> "dict[str, Any] | str":
        """
        Apply a randomly chosen corruption strategy.
        Returns either a corrupted dict or a raw broken JSON string.
        """
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


# -----------------------------------------------------------------------------
# Metrics Collector
# -----------------------------------------------------------------------------

@dataclass
class MetricsCollector:
    """
    Asyncio-safe event counters.
    All increments happen in the single event loop thread, so no locks needed.
    """
    sent: int       = 0
    success: int    = 0
    errors: int     = 0
    chaos_sent: int = 0
    _start: float   = field(default_factory=time.monotonic)

    def record_success(self, *, is_chaos: bool = False) -> None:
        self.sent += 1
        self.success += 1
        if is_chaos:
            self.chaos_sent += 1

    def record_error(self, *, is_chaos: bool = False) -> None:
        self.sent += 1
        self.errors += 1
        if is_chaos:
            self.chaos_sent += 1

    @property
    def elapsed_s(self) -> float:
        return time.monotonic() - self._start

    @property
    def throughput(self) -> float:
        elapsed = self.elapsed_s
        return round(self.sent / elapsed, 2) if elapsed > 0 else 0.0

    def report(self) -> str:
        return (
            f"Elapsed {self.elapsed_s:7.1f}s | "
            f"Sent {self.sent:>8,d} | "
            f"OK {self.success:>8,d} | "
            f"Err {self.errors:>6,d} | "
            f"Chaos {self.chaos_sent:>6,d} | "
            f"{self.throughput:>7.1f} req/s"
        )


# -----------------------------------------------------------------------------
# HTTP Event Sender
# -----------------------------------------------------------------------------

class EventSender:
    """
    Handles low-level HTTP dispatch with retry + exponential backoff.
    Shares a single aiohttp.ClientSession for efficient TCP connection pooling.
    """

    def __init__(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        timeout_s: float,
        max_retries: int,
        logger: logging.Logger,
    ) -> None:
        self._session    = session
        self._endpoint   = endpoint
        self._timeout    = aiohttp.ClientTimeout(total=timeout_s)
        self._max_retries = max_retries
        self._log        = logger.getChild("sender")

    async def send(
        self,
        payload: "dict[str, Any] | str",
        metrics: MetricsCollector,
        *,
        is_chaos: bool = False,
    ) -> None:
        """
        POST the payload with retry logic.
        - dict payloads are JSON-serialised normally.
        - str payloads (broken JSON) are sent as-is for chaos testing.
        - 2xx and 4xx are both recorded as 'success' from the simulator POV
          because they are expected API responses (validation errors are fine).
        - 5xx triggers a retry with exponential backoff.
        """
        body = payload if isinstance(payload, str) else json.dumps(payload)
        headers = {"Content-Type": "application/json"}

        for attempt in range(1, self._max_retries + 1):
            try:
                async with self._session.post(
                    self._endpoint,
                    data=body,
                    headers=headers,
                    timeout=self._timeout,
                ) as resp:
                    if resp.status < 500:
                        metrics.record_success(is_chaos=is_chaos)
                        return
                    self._log.warning(
                        "5xx (%d) on attempt %d/%d", resp.status, attempt, self._max_retries
                    )

            except (aiohttp.ClientConnectionError, asyncio.TimeoutError) as exc:
                self._log.debug(
                    "Network error attempt %d/%d: %s", attempt, self._max_retries, exc
                )

            if attempt < self._max_retries:
                await asyncio.sleep(0.1 * (2 ** (attempt - 1)))  # 100ms, 200ms, 400ms…

        metrics.record_error(is_chaos=is_chaos)


# -----------------------------------------------------------------------------
# Virtual User Worker Coroutine
# -----------------------------------------------------------------------------

async def run_user_worker(
    user: SimulatedUser,
    sender: EventSender,
    chaos: ChaosInjector,
    metrics: MetricsCollector,
    stop_event: asyncio.Event,
    think_time_range: tuple[float, float] = (0.05, 0.3),
) -> None:
    """
    Drives one SimulatedUser through their full journey indefinitely
    until `stop_event` is set. Each loop iteration:
      1. Advance the user's state machine
      2. Build a clean payload
      3. Optionally corrupt it (chaos injection)
      4. POST it to the ingest API
      5. Sleep a randomised think-time simulating human pacing
    """
    while not stop_event.is_set():
        user.advance()
        payload: dict[str, Any] = user.build_payload()

        is_chaos = chaos.should_inject()
        final_payload = chaos.inject(payload) if is_chaos else payload

        await sender.send(final_payload, metrics, is_chaos=is_chaos)
        await asyncio.sleep(random.uniform(*think_time_range))


# -----------------------------------------------------------------------------
# Traffic Controller (Spike Engine)
# -----------------------------------------------------------------------------

class TrafficController:
    """
    Manages the pool of active SimulatedUser coroutines.
    Implements:
      - Normal baseline traffic at `base_workers` concurrency
      - Periodic flash-sale spikes scaled to `spike_workers`
      - Graceful scale-down and teardown
    """

    def __init__(
        self,
        config: Config,
        sender: EventSender,
        chaos: ChaosInjector,
        metrics: MetricsCollector,
        stop_event: asyncio.Event,
        logger: logging.Logger,
    ) -> None:
        self._cfg       = config
        self._sender    = sender
        self._chaos     = chaos
        self._metrics   = metrics
        self._stop      = stop_event
        self._log       = logger.getChild("traffic")
        self._tasks: list[asyncio.Task[None]] = []

    # ------------------------------------------------------------------
    # Internal helpers

    def _spawn(self, count: int, think_range: tuple[float, float]) -> None:
        """Create `count` new virtual user tasks."""
        for _ in range(count):
            user = SimulatedUser()
            task = asyncio.create_task(
                run_user_worker(user, self._sender, self._chaos,
                                self._metrics, self._stop, think_range),
                name=f"user-{user.user_id}",
            )
            self._tasks.append(task)

    def _cancel_last(self, count: int) -> None:
        """Cancel the most-recently spawned `count` tasks (LIFO)."""
        to_cancel, self._tasks = self._tasks[-count:], self._tasks[:-count]
        for t in to_cancel:
            t.cancel()

    # ------------------------------------------------------------------
    # Main loop

    async def run(self) -> None:
        """
        Control loop:
          1. Start baseline workers
          2. Sleep until the next spike window
          3. Trigger flash-sale spike
          4. Repeat until stop_event is set
        """
        self._log.info(
            "Starting baseline traffic | workers=%d | think=[%.2f, %.2f]s",
            self._cfg.base_workers, 0.1, 0.4,
        )
        self._spawn(self._cfg.base_workers, think_range=(0.1, 0.4))

        while not self._stop.is_set():
            # Wait for the next spike; exit cleanly if stop fires first
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._stop.wait()),
                    timeout=self._cfg.spike_interval_s,
                )
                break
            except asyncio.TimeoutError:
                pass

            if not self._stop.is_set():
                await self._trigger_spike()

    async def _trigger_spike(self) -> None:
        """
        Flash-sale simulation:
          - Add extra workers with tight think-times (frantic user clicks)
          - Hold the spike for `spike_duration_s`
          - Scale back down to baseline
        """
        extra = self._cfg.spike_workers - self._cfg.base_workers
        self._log.warning(
            "FLASH SALE SPIKE | %d -> %d workers | duration=%.0fs",
            self._cfg.base_workers, self._cfg.spike_workers, self._cfg.spike_duration_s,
        )
        self._spawn(extra, think_range=(0.005, 0.05))  # near-zero think time = max pressure

        try:
            await asyncio.wait_for(
                asyncio.shield(self._stop.wait()),
                timeout=self._cfg.spike_duration_s,
            )
        except asyncio.TimeoutError:
            pass  # normal — spike duration elapsed

        self._log.info(
            "Spike ended | scaling back to %d workers", self._cfg.base_workers
        )
        self._cancel_last(extra)

    # ------------------------------------------------------------------
    # Teardown

    async def teardown(self) -> None:
        """Cancel all tasks and wait for them to finish cleanly."""
        self._log.info("Cancelling %d worker tasks…", len(self._tasks))
        for t in self._tasks:
            t.cancel()
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
        self._log.info("Teardown complete | %d tasks cancelled", cancelled)


# -----------------------------------------------------------------------------
# Live Metrics Reporter
# -----------------------------------------------------------------------------

async def run_metrics_reporter(
    metrics: MetricsCollector,
    interval_s: float,
    stop_event: asyncio.Event,
    logger: logging.Logger,
) -> None:
    """Periodically emits a throughput snapshot to the log."""
    log = logger.getChild("metrics")
    while not stop_event.is_set():
        await asyncio.sleep(interval_s)
        log.info(metrics.report())


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------

async def main() -> None:
    config = Config()
    logger = _setup_logging(config.log_level)

    logger.info("=" * 68)
    logger.info("  TapDrink Chaos Simulator — Initialising")
    logger.info("=" * 68)
    logger.info("  Target endpoint   : %s", config.ingest_endpoint)
    logger.info("  Base workers      : %d", config.base_workers)
    logger.info("  Spike workers     : %d  (%dx multiplier)",
                config.spike_workers, config.spike_multiplier)
    logger.info("  Spike schedule    : %.0fs duration every %.0fs",
                config.spike_duration_s, config.spike_interval_s)
    logger.info("  Chaos ratio       : %.0f%%", config.chaos_ratio * 100)
    logger.info("  Timeout / retries : %.1fs / %d",
                config.request_timeout_s, config.max_retries)
    logger.info("=" * 68)

    stop_event = asyncio.Event()
    metrics    = MetricsCollector()
    chaos      = ChaosInjector(config.chaos_ratio, logger)

    # One shared TCP connection pool across ALL workers for efficiency
    connector = aiohttp.TCPConnector(
        limit=config.spike_workers + 100,  # headroom above peak concurrency
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        sender = EventSender(
            session     = session,
            endpoint    = config.ingest_endpoint,
            timeout_s   = config.request_timeout_s,
            max_retries = config.max_retries,
            logger      = logger,
        )
        controller = TrafficController(
            config    = config,
            sender    = sender,
            chaos     = chaos,
            metrics   = metrics,
            stop_event= stop_event,
            logger    = logger,
        )
        reporter = asyncio.create_task(
            run_metrics_reporter(metrics, config.report_interval_s, stop_event, logger),
            name="metrics-reporter",
        )

        try:
            await controller.run()
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Interrupt received — graceful shutdown initiated…")
        finally:
            stop_event.set()
            await controller.teardown()
            reporter.cancel()
            await asyncio.gather(reporter, return_exceptions=True)

    # Final summary
    logger.info("=" * 68)
    logger.info("  FINAL REPORT")
    logger.info("  %s", metrics.report())
    logger.info("=" * 68)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
