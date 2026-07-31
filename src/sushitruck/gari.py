"""gari — rate limiting, retry with backoff, and circuit breaking. Pure stdlib."""

from __future__ import annotations

import functools
import random
import threading
import time
from typing import Any, Callable, TypeVar

F = TypeVar("F", bound=Callable[..., Any])


def backoff_sleep(attempt: int, base: float = 2.0, *, jitter: bool = True) -> float:
    """
    Sleep for an exponential backoff interval and return the duration slept.

    Parameters
    ----------
    attempt
        Retry attempt number, starting at 1 for the first retry.
    base
        Base of the exponential backoff; sleep duration is ``base ** attempt``.
    jitter
        Whether to multiply the sleep duration by a random factor in
        ``[0.5, 1.5)`` to spread out retries from concurrent callers.

    Returns
    -------
    float
        The number of seconds actually slept.
    """

    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")

    duration = base ** attempt
    if jitter:
        duration *= random.uniform(0.5, 1.5)

    time.sleep(duration)
    return duration


class RateLimiter:
    """Thread-safe token-bucket rate limiter, reusable across repeated calls."""

    def __init__(self, calls_per_second: float) -> None:
        if calls_per_second <= 0:
            raise ValueError(f"calls_per_second must be > 0, got {calls_per_second}")

        self._rate = calls_per_second
        self._capacity = max(1.0, calls_per_second)
        self._tokens = self._capacity
        self._last_check = time.monotonic()
        self._lock = threading.Lock()

    def sleep_until_ready(self) -> float:
        """Block until a token is available, then consume it. Returns time slept."""

        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_check
            self._last_check = now
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return 0.0

            wait = (1.0 - self._tokens) / self._rate
            self._tokens = 0.0

        time.sleep(wait)
        with self._lock:
            self._last_check = time.monotonic()
        return wait


def sleep_until_ready(calls_per_second: float) -> float:
    """
    Block until a fresh, one-off token bucket at ``calls_per_second`` allows a call.

    This constructs a new bucket on every call, so it always returns
    immediately. It exists for quick scripting use; prefer :func:`rate_limit`
    for a persistent limiter shared across repeated calls.

    Parameters
    ----------
    calls_per_second
        Maximum call rate to honor.

    Returns
    -------
    float
        The number of seconds slept (always ``0.0`` for a fresh bucket).
    """

    return RateLimiter(calls_per_second).sleep_until_ready()


def rate_limit(calls_per_second: float) -> Callable[[F], F]:
    """
    Decorate a function so calls are throttled to a token-bucket rate limit.

    Parameters
    ----------
    calls_per_second
        Maximum sustained call rate. Bursts up to this many calls per second
        are allowed; excess calls block until a token frees up.
    """

    def decorator(func: F) -> F:
        bucket = RateLimiter(calls_per_second)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            bucket.sleep_until_ready()
            return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


def _status_code_of(result: Any) -> int | None:
    """Extract an HTTP status code from a requests.Response-like result."""

    return getattr(result, "status_code", None)


def retry(
    max_attempts: int = 3,
    *,
    backoff_base: float = 2.0,
    jitter: bool = True,
    retry_on: tuple[Any, ...] = (429, 500, 502, 503, 504),
) -> Callable[[F], F]:
    """
    Decorate a function to retry on failure with exponential backoff.

    Parameters
    ----------
    max_attempts
        Total number of attempts, including the first (non-retry) call.
    backoff_base
        Base of the exponential backoff between attempts.
    jitter
        Whether to randomize the backoff duration.
    retry_on
        HTTP status codes (checked against a returned ``requests.Response``'s
        ``status_code``) and/or exception types that should trigger a retry.
        Anything else — a clean return, or an exception not listed — passes
        straight through.
    """

    if max_attempts < 1:
        raise ValueError(f"max_attempts must be >= 1, got {max_attempts}")

    status_codes = {v for v in retry_on if isinstance(v, int)}
    exception_types = tuple(v for v in retry_on if isinstance(v, type) and issubclass(v, BaseException))

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            last_exc: BaseException | None = None
            last_result: Any = None

            for attempt in range(1, max_attempts + 1):
                try:
                    result = func(*args, **kwargs)
                except exception_types as exc:
                    last_exc = exc
                    if attempt == max_attempts:
                        raise
                    backoff_sleep(attempt, base=backoff_base, jitter=jitter)
                    continue

                if _status_code_of(result) not in status_codes:
                    return result

                last_result = result
                if attempt == max_attempts:
                    return last_result
                backoff_sleep(attempt, base=backoff_base, jitter=jitter)

            if last_exc is not None:
                raise last_exc
            return last_result

        return wrapper  # type: ignore[return-value]

    return decorator


class CircuitBreakerOpenError(RuntimeError):
    """Raised when a call is rejected because the circuit is open."""


class _CircuitBreaker:
    """Per-function circuit breaker state machine: closed, open, half-open."""

    def __init__(self, failure_threshold: int, recovery_timeout: float) -> None:
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._failures = 0
        self._opened_at: float | None = None
        self._lock = threading.Lock()

    def _state(self) -> str:
        if self._opened_at is None:
            return "closed"
        if time.monotonic() - self._opened_at >= self._recovery_timeout:
            return "half-open"
        return "open"

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        with self._lock:
            state = self._state()
            if state == "open":
                raise CircuitBreakerOpenError(
                    "Circuit breaker is open; call rejected without attempting it."
                )

        try:
            result = func(*args, **kwargs)
        except Exception:
            with self._lock:
                self._failures += 1
                if self._failures >= self._failure_threshold:
                    self._opened_at = time.monotonic()
            raise
        else:
            with self._lock:
                self._failures = 0
                self._opened_at = None
            return result


def circuit_breaker(
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
) -> Callable[[F], F]:
    """
    Decorate a function with fail-fast circuit-breaker protection.

    Three states apply per decorated function (per process): closed (normal
    operation, failures counted), open (calls fail immediately once
    ``failure_threshold`` consecutive failures occur), and half-open (after
    ``recovery_timeout`` seconds, the next call is tried; success closes the
    circuit, failure re-opens it).

    Parameters
    ----------
    failure_threshold
        Consecutive failures required to open the circuit.
    recovery_timeout
        Seconds to wait before allowing a trial call while open.

    Raises
    ------
    CircuitBreakerOpenError
        If the circuit is open when the wrapped function is called.
    """

    if failure_threshold < 1:
        raise ValueError(f"failure_threshold must be >= 1, got {failure_threshold}")

    def decorator(func: F) -> F:
        breaker = _CircuitBreaker(failure_threshold, recovery_timeout)

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return breaker.call(func, *args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator
