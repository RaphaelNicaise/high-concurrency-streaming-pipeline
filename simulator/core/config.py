import os
from dataclasses import dataclass, field

@dataclass(frozen=True)
class Config:
    ingest_api_url: str = field(
        default_factory=lambda: os.getenv("INGEST_API_URL", "http://localhost:8000")
    )
    base_workers: int = field(
        default_factory=lambda: int(os.getenv("BASE_WORKERS", "50"))
    )
    spike_multiplier: int = field(
        default_factory=lambda: int(os.getenv("SPIKE_MULTIPLIER", "10"))
    )
    spike_duration_s: float = field(
        default_factory=lambda: float(os.getenv("SPIKE_DURATION_S", "30"))
    )
    spike_interval_s: float = field(
        default_factory=lambda: float(os.getenv("SPIKE_INTERVAL_S", "120"))
    )
    chaos_ratio: float = field(
        default_factory=lambda: float(os.getenv("CHAOS_RATIO", "0.05"))
    )
    request_timeout_s: float = field(
        default_factory=lambda: float(os.getenv("REQUEST_TIMEOUT_S", "10"))
    )
    report_interval_s: float = field(
        default_factory=lambda: float(os.getenv("REPORT_INTERVAL_S", "5"))
    )
    max_retries: int = field(default_factory=lambda: int(os.getenv("MAX_RETRIES", "3")))
    log_level: str = field(default_factory=lambda: os.getenv("LOG_LEVEL", "INFO"))

    @property
    def ingest_endpoint(self) -> str:
        return f"{self.ingest_api_url.rstrip('/')}/events"

    @property
    def control_endpoint(self) -> str:
        return f"{self.ingest_api_url.rstrip('/')}/simulator/control"

    @property
    def spike_workers(self) -> int:
        return self.base_workers * self.spike_multiplier
