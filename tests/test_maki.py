import responses

from sushitruck import maki


@responses.activate
def test_get_returns_parsed_json():
    responses.add(responses.GET, "https://api.example.com/v2/prices", json={"price": 1.5})

    client = maki.MakiClient("https://api.example.com/v2")
    assert client.get("/prices") == {"price": 1.5}


@responses.activate
def test_bearer_auth_header_sent():
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})

    client = maki.MakiClient("https://api.example.com", auth={"type": "bearer", "token": "tok123"})
    client.get("/ping")

    assert responses.calls[0].request.headers["Authorization"] == "Bearer tok123"


@responses.activate
def test_basic_auth_header_sent():
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})

    client = maki.MakiClient(
        "https://api.example.com", auth={"type": "basic", "username": "u", "password": "p"}
    )
    client.get("/ping")

    assert responses.calls[0].request.headers["Authorization"].startswith("Basic ")


@responses.activate
def test_api_key_header_sent():
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})

    client = maki.MakiClient(
        "https://api.example.com", auth={"type": "api_key", "key": "abc", "header": "X-API-Key"}
    )
    client.get("/ping")

    assert responses.calls[0].request.headers["X-API-Key"] == "abc"


@responses.activate
def test_api_key_query_param_sent():
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})

    client = maki.MakiClient(
        "https://api.example.com", auth={"type": "api_key", "key": "abc", "param": "api_key"}
    )
    client.get("/ping")

    assert "api_key=abc" in responses.calls[0].request.url


@responses.activate
def test_rejects_unsupported_auth_type():
    import pytest

    with pytest.raises(ValueError):
        maki.MakiClient("https://api.example.com", auth={"type": "bogus"})


@responses.activate
def test_post_sends_json_body():
    responses.add(responses.POST, "https://api.example.com/items", json={"id": 1})

    client = maki.MakiClient("https://api.example.com")
    result = client.post("/items", json={"name": "widget"})

    assert result == {"id": 1}
    assert responses.calls[0].request.body == b'{"name": "widget"}'


@responses.activate
def test_get_paginated_page_number_strategy():
    responses.add(responses.GET, "https://api.example.com/items", json=[{"id": 1}, {"id": 2}])
    responses.add(responses.GET, "https://api.example.com/items", json=[{"id": 3}])
    responses.add(responses.GET, "https://api.example.com/items", json=[])

    client = maki.MakiClient("https://api.example.com")
    results = client.get("/items", paginate=True, page_size=2)

    assert results == [{"id": 1}, {"id": 2}, {"id": 3}]


@responses.activate
def test_get_paginated_cursor_strategy():
    responses.add(
        responses.GET,
        "https://api.example.com/items",
        json={"data": [{"id": 1}], "next_cursor": "abc"},
    )
    responses.add(
        responses.GET,
        "https://api.example.com/items",
        json={"data": [{"id": 2}], "next_cursor": None},
    )

    client = maki.MakiClient("https://api.example.com")
    results = client.get("/items", paginate=True, results_key="data")

    assert results == [{"id": 1}, {"id": 2}]


@responses.activate
def test_fetch_returns_flattened_dataframe():
    responses.add(
        responses.GET,
        "https://api.example.com/prices",
        json=[{"price": 1.0, "meta": {"source": "x"}}],
    )

    client = maki.MakiClient("https://api.example.com")
    df = client.fetch("/prices")

    assert "meta_source" in df.columns
    assert df.iloc[0]["price"] == 1.0


@responses.activate
def test_client_respects_rate_limit(monkeypatch):
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})

    waited = {"count": 0}
    client = maki.MakiClient("https://api.example.com", rate_limit=100)
    monkeypatch.setattr(client._limiter, "sleep_until_ready", lambda: waited.__setitem__("count", waited["count"] + 1))

    client.get("/ping")
    assert waited["count"] == 1


@responses.activate
def test_retries_on_retryable_status(monkeypatch):
    from sushitruck import gari

    monkeypatch.setattr(gari, "backoff_sleep", lambda *a, **k: 0.0)

    responses.add(responses.GET, "https://api.example.com/flaky", status=503)
    responses.add(responses.GET, "https://api.example.com/flaky", json={"ok": True})

    client = maki.MakiClient("https://api.example.com", retries=3)
    result = client.get("/flaky")
    assert result == {"ok": True}
