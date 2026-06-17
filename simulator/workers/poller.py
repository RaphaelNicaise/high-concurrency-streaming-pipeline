import asyncio
import logging
import aiohttp
from core.state import SimulatorState
from services.metrics import MetricsCollector

async def run_metrics_reporter(
    metrics: MetricsCollector,
    interval_s: float,
    stop_event: asyncio.Event,
    logger: logging.Logger,
) -> None:
    log = logger.getChild("metrics")
    while not stop_event.is_set():
        await asyncio.sleep(interval_s)
        log.info(metrics.report())

async def run_state_poller(
    session: aiohttp.ClientSession,
    endpoint: str,
    state: SimulatorState,
    stop_event: asyncio.Event,
    logger: logging.Logger,
) -> None:
    log = logger.getChild("control")
    while not stop_event.is_set():
        try:
            async with session.get(endpoint, timeout=2.0) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    new_paused = data.get("state") == "PAUSED"
                    if new_paused != state.is_paused:
                        log.warning(
                            f"State changed: {'PAUSED' if new_paused else 'RUNNING'}"
                        )
                        state.is_paused = new_paused
        except Exception as e:
            log.debug(f"Failed to fetch control state: {e}")

        await asyncio.sleep(1.0)
