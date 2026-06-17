import asyncio
import logging
from typing import Any

import aiohttp
from services.metrics import MetricsCollector

class EventSender:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        endpoint: str,
        timeout_s: float,
        max_retries: int,
        logger: logging.Logger,
    ) -> None:
        self.session = session
        self.endpoint = endpoint
        self.timeout_s = timeout_s
        self.max_retries = max_retries
        self._log = logger.getChild("sender")

    async def send(
        self, payload: dict[str, Any], metrics: MetricsCollector, is_chaos: bool
    ) -> None:
        retries = 0
        while retries <= self.max_retries:
            try:
                async with self.session.post(
                    self.endpoint,
                    json=payload,
                    timeout=self.timeout_s,
                ) as response:
                    if response.status in (202, 200, 201):
                        metrics.record_success(is_chaos=is_chaos)
                        return
                    else:
                        text = await response.text()
                        self._log.debug(
                            f"HTTP {response.status} sending event: {text}"
                        )
            except asyncio.TimeoutError:
                self._log.debug("Timeout sending event")
            except aiohttp.ClientError as e:
                self._log.debug(f"ClientError sending event: {e}")
            except Exception as e:
                self._log.debug(f"Unknown error sending event: {e}")

            retries += 1
            if retries <= self.max_retries:
                await asyncio.sleep(0.5 * (2 ** (retries - 1)))

        # If we got here, all retries failed
        metrics.record_error(is_chaos=is_chaos)
