from dataclasses import dataclass


@dataclass
class HealthStatus:
    ok: bool
    details: dict


def check_health() -> HealthStatus:
    return HealthStatus(ok=True, details={"status": "ok"})
