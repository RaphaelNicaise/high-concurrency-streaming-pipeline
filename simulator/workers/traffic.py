import asyncio
import logging
import random
from typing import Any

from core.config import Config
from core.state import SimulatorState
from models.user import SimulatedUser
from services.chaos import ChaosInjector
from services.metrics import MetricsCollector
from services.sender import EventSender

async def run_user_worker(
    user: SimulatedUser,
    sender: EventSender,
    chaos: ChaosInjector,
    metrics: MetricsCollector,
    state: SimulatorState,
    stop_event: asyncio.Event,
    think_time_range: tuple[float, float] = (0.05, 0.3),
) -> None:
    while not stop_event.is_set():
        if state.is_paused:
            await asyncio.sleep(1.0)
            continue

        user.advance()
        payload: dict[str, Any] = user.build_payload()

        is_chaos = chaos.should_inject()
        final_payload = chaos.inject(payload) if is_chaos else payload

        await sender.send(final_payload, metrics, is_chaos=is_chaos)
        await asyncio.sleep(random.uniform(*think_time_range))

class TrafficController:
    def __init__(
        self,
        config: Config,
        sender: EventSender,
        chaos: ChaosInjector,
        metrics: MetricsCollector,
        state: SimulatorState,
        stop_event: asyncio.Event,
        logger: logging.Logger,
    ) -> None:
        self._cfg = config
        self._sender = sender
        self._chaos = chaos
        self._metrics = metrics
        self._state = state
        self._stop = stop_event
        self._log = logger.getChild("traffic")
        self._tasks: list[asyncio.Task[None]] = []

    def _spawn(self, count: int, think_range: tuple[float, float]) -> None:
        for _ in range(count):
            user = SimulatedUser()
            task = asyncio.create_task(
                run_user_worker(
                    user,
                    self._sender,
                    self._chaos,
                    self._metrics,
                    self._state,
                    self._stop,
                    think_range,
                ),
                name=f"user-{user.user_id}",
            )
            self._tasks.append(task)

    def _cancel_last(self, count: int) -> None:
        to_cancel, self._tasks = self._tasks[-count:], self._tasks[:-count]
        for t in to_cancel:
            t.cancel()

    async def run(self) -> None:
        self._log.info(
            "Starting baseline traffic | workers=%d | think=[%.2f, %.2f]s",
            self._cfg.base_workers,
            0.1,
            0.4,
        )
        self._spawn(self._cfg.base_workers, think_range=(0.1, 0.4))

        while not self._stop.is_set():
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
        extra = self._cfg.spike_workers - self._cfg.base_workers
        self._log.warning(
            "FLASH SALE SPIKE | %d -> %d workers | duration=%.0fs",
            self._cfg.base_workers,
            self._cfg.spike_workers,
            self._cfg.spike_duration_s,
        )
        self._spawn(extra, think_range=(0.005, 0.05))

        try:
            await asyncio.wait_for(
                asyncio.shield(self._stop.wait()),
                timeout=self._cfg.spike_duration_s,
            )
        except asyncio.TimeoutError:
            pass 

        self._log.info(
            "Spike ended | scaling back to %d workers", self._cfg.base_workers
        )
        self._cancel_last(extra)

    async def teardown(self) -> None:
        self._log.info("Cancelling %d worker tasks…", len(self._tasks))
        for t in self._tasks:
            t.cancel()
        results = await asyncio.gather(*self._tasks, return_exceptions=True)
        cancelled = sum(1 for r in results if isinstance(r, asyncio.CancelledError))
        self._log.info("Teardown complete | %d tasks cancelled", cancelled)
