import time

import pytest
import requests

from sushitruck import gari


def test_backoff_sleep_scales_with_attempt(monkeypatch):
    slept = []
    monkeypatch.setattr(time, "sleep", lambda s: slept.append(s))

    gari.backoff_sleep(attempt=2, base=2.0, jitter=False)

    assert slept == [4.0]


def test_backoff_sleep_rejects_invalid_attempt():
    with pytest.raises(ValueError):
        gari.backoff_sleep(attempt=0)


def test_rate_limiter_throttles_bursts():
    limiter = gari.RateLimiter(calls_per_second=1000)
    waits = [limiter.sleep_until_ready() for _ in range(5)]
    assert all(w >= 0 for w in waits)


def test_rate_limit_decorator_rejects_non_positive():
    with pytest.raises(ValueError):
        gari.rate_limit(0)(lambda: None)


def test_rate_limit_decorator_calls_through():
    calls = []

    @gari.rate_limit(calls_per_second=1000)
    def fn(x):
        calls.append(x)
        return x * 2

    assert fn(3) == 6
    assert calls == [3]


def test_retry_retries_on_status_code(monkeypatch):
    monkeypatch.setattr(gari, "backoff_sleep", lambda *a, **k: 0.0)

    attempts = {"count": 0}

    def make_response(status):
        resp = requests.Response()
        resp.status_code = status
        return resp

    @gari.retry(max_attempts=3, retry_on=(500,))
    def flaky():
        attempts["count"] += 1
        return make_response(500 if attempts["count"] < 3 else 200)

    result = flaky()
    assert result.status_code == 200
    assert attempts["count"] == 3


def test_retry_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setattr(gari, "backoff_sleep", lambda *a, **k: 0.0)

    def always_500():
        resp = requests.Response()
        resp.status_code = 500
        return resp

    wrapped = gari.retry(max_attempts=2, retry_on=(500,))(always_500)
    result = wrapped()
    assert result.status_code == 500


def test_retry_retries_on_exception_type(monkeypatch):
    monkeypatch.setattr(gari, "backoff_sleep", lambda *a, **k: 0.0)

    attempts = {"count": 0}

    @gari.retry(max_attempts=3, retry_on=(ConnectionError,))
    def flaky():
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise ConnectionError("boom")
        return "ok"

    assert flaky() == "ok"
    assert attempts["count"] == 2


def test_retry_reraises_unlisted_exception():
    @gari.retry(max_attempts=3, retry_on=(ConnectionError,))
    def broken():
        raise ValueError("not retryable")

    with pytest.raises(ValueError):
        broken()


def test_retry_passes_through_success_immediately():
    calls = {"count": 0}

    @gari.retry(max_attempts=5)
    def fn():
        calls["count"] += 1
        return "done"

    assert fn() == "done"
    assert calls["count"] == 1


def test_circuit_breaker_opens_after_threshold():
    @gari.circuit_breaker(failure_threshold=2, recovery_timeout=60)
    def always_fails():
        raise RuntimeError("nope")

    with pytest.raises(RuntimeError):
        always_fails()
    with pytest.raises(RuntimeError):
        always_fails()

    with pytest.raises(gari.CircuitBreakerOpenError):
        always_fails()


def test_circuit_breaker_recovers_after_timeout(monkeypatch):
    state = {"fail": True}

    @gari.circuit_breaker(failure_threshold=1, recovery_timeout=0.05)
    def sometimes_fails():
        if state["fail"]:
            raise RuntimeError("nope")
        return "ok"

    with pytest.raises(RuntimeError):
        sometimes_fails()
    with pytest.raises(gari.CircuitBreakerOpenError):
        sometimes_fails()

    time.sleep(0.1)
    state["fail"] = False
    assert sometimes_fails() == "ok"


def test_circuit_breaker_rejects_bad_threshold():
    with pytest.raises(ValueError):
        gari.circuit_breaker(failure_threshold=0)
