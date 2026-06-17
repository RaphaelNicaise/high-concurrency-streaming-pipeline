import asyncio
import aiohttp

from core.config import Config
from core.logger import setup_logging
from core.state import SimulatorState
from services.chaos import ChaosInjector
from services.metrics import MetricsCollector
from services.sender import EventSender
from workers.poller import run_metrics_reporter, run_state_poller
from workers.traffic import TrafficController

async def main() -> None:
    config = Config()
    logger = setup_logging(config.log_level)

    logger.info("=" * 68)
    logger.info("  TapDrink Chaos Simulator — Initialising")
    logger.info("=" * 68)
    logger.info("  Target endpoint   : %s", config.ingest_endpoint)
    logger.info("  Base workers      : %d", config.base_workers)
    logger.info(
        "  Spike workers     : %d  (%dx multiplier)",
        config.spike_workers,
        config.spike_multiplier,
    )
    logger.info(
        "  Spike schedule    : %.0fs duration every %.0fs",
        config.spike_duration_s,
        config.spike_interval_s,
    )
    logger.info("  Chaos ratio       : %.0f%%", config.chaos_ratio * 100)
    logger.info(
        "  Timeout / retries : %.1fs / %d", config.request_timeout_s, config.max_retries
    )
    logger.info("=" * 68)

    stop_event = asyncio.Event()
    metrics = MetricsCollector()
    chaos = ChaosInjector(config.chaos_ratio, logger)
    state = SimulatorState()

    connector = aiohttp.TCPConnector(
        limit=config.spike_workers + 100,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        sender = EventSender(
            session=session,
            endpoint=config.ingest_endpoint,
            timeout_s=config.request_timeout_s,
            max_retries=config.max_retries,
            logger=logger,
        )
        controller = TrafficController(
            config=config,
            sender=sender,
            chaos=chaos,
            metrics=metrics,
            state=state,
            stop_event=stop_event,
            logger=logger,
        )
        reporter = asyncio.create_task(
            run_metrics_reporter(metrics, config.report_interval_s, stop_event, logger),
            name="metrics-reporter",
        )
        poller = asyncio.create_task(
            run_state_poller(
                session, config.control_endpoint, state, stop_event, logger
            ),
            name="state-poller",
        )

        try:
            await controller.run()
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("Interrupt received — graceful shutdown initiated…")
        finally:
            stop_event.set()
            await controller.teardown()
            reporter.cancel()
            poller.cancel()
            await asyncio.gather(reporter, poller, return_exceptions=True)

    logger.info("=" * 68)
    logger.info("  FINAL REPORT")
    logger.info("  %s", metrics.report())
    logger.info("=" * 68)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
