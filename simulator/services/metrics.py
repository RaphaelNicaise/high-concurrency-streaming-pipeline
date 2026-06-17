import time
from dataclasses import dataclass, field

@dataclass
class MetricsCollector:
    sent: int = 0
    success: int = 0
    errors: int = 0
    chaos_sent: int = 0
    _start: float = field(default_factory=time.monotonic)

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
