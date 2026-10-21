"""Internal calendar-day batching primitives."""

from dataclasses import replace
from datetime import UTC, datetime, time

from collekt.core.request import Request


def validate_batch_days(value: int | None) -> None:
    """Reject ambiguous durations and nonpositive batch sizes."""
    if value is not None and (type(value) is not int or value < 1):
        raise ValueError("batch_days must be a positive integer number of UTC calendar days, or None")


def request_windows(request: Request, batch_days: int) -> list[Request]:
    """Partition the inclusive request into clipped UTC calendar-day windows."""
    validate_batch_days(batch_days)
    days = request.iter_days()
    windows = []
    for offset in range(0, len(days), batch_days):
        last = days[min(offset + batch_days, len(days)) - 1]
        windows.append(
            replace(
                request,
                start=max(request.start_datetime, datetime.combine(days[offset], time.min, UTC)),
                end=min(request.end_datetime, datetime.combine(last, time.max, UTC)),
            )
        )
    return windows
