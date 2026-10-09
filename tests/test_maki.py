import pytest
import requests
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


@responses.activate
def test_get_non_paginated_applies_results_key():
    responses.add(responses.GET, "https://api.example.com/items", json={"data": {"items": [{"id": 1}]}})

    client = maki.MakiClient("https://api.example.com")
    assert client.get("/items", results_key="data.items") == [{"id": 1}]


@responses.activate
def test_results_key_missing_raises_key_error():
    responses.add(responses.GET, "https://api.example.com/items", json={"data": []})

    client = maki.MakiClient("https://api.example.com")
    with pytest.raises(KeyError):
        client.get("/items", results_key="data.items")


@responses.activate
def test_get_paginated_page_strategy_with_results_key_stops_on_empty_page():
    for items in ([{"id": 1}, {"id": 2}], [{"id": 3}], []):
        responses.add(responses.GET, "https://api.example.com/items", json={"data": items})

    client = maki.MakiClient("https://api.example.com")
    results = client.get("/items", paginate=True, pagination="page", results_key="data")

    assert results == [{"id": 1}, {"id": 2}, {"id": 3}]
    pages = [responses.calls[i].request.params["page"] for i in range(3)]
    assert pages == ["1", "2", "3"]


@responses.activate
def test_get_paginated_offset_strategy():
    responses.add(responses.GET, "https://api.example.com/rows", json=[{"id": 1}, {"id": 2}])
    responses.add(responses.GET, "https://api.example.com/rows", json=[{"id": 3}, {"id": 4}])
    responses.add(responses.GET, "https://api.example.com/rows", json=[{"id": 5}])

    client = maki.MakiClient("https://api.example.com")
    results = client.get("/rows", paginate=True, pagination="offset", page_size=2)

    assert [r["id"] for r in results] == [1, 2, 3, 4, 5]
    # A short final page ends the walk: no fourth request.
    assert len(responses.calls) == 3
    sent = [responses.calls[i].request.params for i in range(3)]
    assert [p["offset"] for p in sent] == ["0", "2", "4"]
    assert all(p["limit"] == "2" for p in sent)
    assert all("page" not in p for p in sent)


@responses.activate
def test_get_paginated_offset_strategy_custom_param_names():
    responses.add(responses.GET, "https://api.example.com/rows", json=[{"id": 1}])

    client = maki.MakiClient("https://api.example.com")
    client.get(
        "/rows",
        paginate=True,
        pagination="offset",
        offset_param="skip",
        page_size_param="take",
        page_size=10,
    )

    assert responses.calls[0].request.params == {"skip": "0", "take": "10"}


@responses.activate
def test_get_paginated_offset_strategy_stops_on_empty_page():
    responses.add(responses.GET, "https://api.example.com/rows", json=[{"id": 1}, {"id": 2}])
    responses.add(responses.GET, "https://api.example.com/rows", json=[])

    client = maki.MakiClient("https://api.example.com")
    results = client.get("/rows", paginate=True, pagination="offset", page_size=2)

    assert len(results) == 2
    assert len(responses.calls) == 2


@responses.activate
def test_cursor_pagination_sends_cursor_and_drops_page_param():
    responses.add(
        responses.GET, "https://api.example.com/items", json={"data": [{"id": 1}], "next_cursor": "c1"}
    )
    responses.add(
        responses.GET, "https://api.example.com/items", json={"data": [{"id": 2}], "next_token": "c2"}
    )
    responses.add(responses.GET, "https://api.example.com/items", json={"data": [{"id": 3}]})

    client = maki.MakiClient("https://api.example.com")
    results = client.get("/items", paginate=True, results_key="data", cursor_param="after")

    assert [r["id"] for r in results] == [1, 2, 3]
    first, second, third = (responses.calls[i].request.params for i in range(3))
    assert "after" not in first
    assert second["after"] == "c1" and "page" not in second
    assert third["after"] == "c2" and "page" not in third


@responses.activate
def test_explicit_cursor_strategy_omits_page_param_on_first_request():
    responses.add(responses.GET, "https://api.example.com/items", json={"data": [{"id": 1}]})

    client = maki.MakiClient("https://api.example.com")
    client.get("/items", paginate=True, pagination="cursor", results_key="data")

    assert "page" not in responses.calls[0].request.params


NEXT_PAGE_LINK = '<https://api.example.com/items?page=2>; rel="next"'


@responses.activate
def test_link_header_pagination_follows_absolute_next_url():
    responses.add(
        responses.GET, "https://api.example.com/items", json=[{"id": 1}], headers={"Link": NEXT_PAGE_LINK}
    )
    responses.add(responses.GET, "https://api.example.com/items", json=[{"id": 2}])

    client = maki.MakiClient("https://api.example.com")
    results = client.get("/items", paginate=True, pagination="link")

    assert results == [{"id": 1}, {"id": 2}]
    assert responses.calls[1].request.url.startswith("https://api.example.com/items?")
    assert responses.calls[1].request.params["page"] == "2"


@responses.activate
def test_auto_pagination_switches_to_link_header():
    responses.add(
        responses.GET, "https://api.example.com/items", json=[{"id": 1}], headers={"Link": NEXT_PAGE_LINK}
    )
    responses.add(responses.GET, "https://api.example.com/items", json=[{"id": 2}])

    client = maki.MakiClient("https://api.example.com")
    results = client.get("/items", paginate=True)

    assert results == [{"id": 1}, {"id": 2}]
    assert len(responses.calls) == 2


def test_rejects_unknown_pagination_strategy():
    client = maki.MakiClient("https://api.example.com")
    with pytest.raises(ValueError):
        client.get("/items", paginate=True, pagination="telepathy")


@responses.activate
def test_fetch_paginated_extracts_results_key_from_every_page():
    responses.add(
        responses.GET,
        "https://api.example.com/tx",
        json={"data": {"transactions": [{"id": 1, "meta": {"src": "a"}}]}},
    )
    responses.add(
        responses.GET,
        "https://api.example.com/tx",
        json={"data": {"transactions": [{"id": 2, "meta": {"src": "b"}}]}},
    )
    responses.add(responses.GET, "https://api.example.com/tx", json={"data": {"transactions": []}})

    client = maki.MakiClient("https://api.example.com")
    df = client.fetch("/tx", paginate=True, page_size=200, results_key="data.transactions")

    assert df["id"].tolist() == [1, 2]
    assert df["meta_src"].tolist() == ["a", "b"]
    assert responses.calls[0].request.params["per_page"] == "200"


@responses.activate
def test_fetch_non_paginated_results_key_and_no_flatten():
    responses.add(
        responses.GET, "https://api.example.com/one", json={"data": {"id": 7, "meta": {"x": 1}}}
    )

    client = maki.MakiClient("https://api.example.com")
    df = client.fetch("/one", results_key="data", flatten=False)

    assert len(df) == 1
    assert df.iloc[0]["meta"] == {"x": 1}


OAUTH2 = {
    "type": "oauth2",
    "token_url": "https://auth.example.com/token",
    "client_id": "id",
    "client_secret": "secret",
}


@responses.activate
def test_oauth2_fetches_token_and_caches_it():
    responses.add(
        responses.POST, "https://auth.example.com/token", json={"access_token": "tok-1", "expires_in": 3600}
    )
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})

    client = maki.MakiClient("https://api.example.com", auth=OAUTH2)
    client.get("/ping")
    client.get("/ping")

    token_calls = [c for c in responses.calls if "auth.example.com" in c.request.url]
    api_calls = [c for c in responses.calls if "api.example.com" in c.request.url]
    assert len(token_calls) == 1
    assert "grant_type=client_credentials" in token_calls[0].request.body
    assert all(c.request.headers["Authorization"] == "Bearer tok-1" for c in api_calls)


@responses.activate
def test_oauth2_refreshes_expired_token():
    responses.add(
        responses.POST, "https://auth.example.com/token", json={"access_token": "old", "expires_in": 0}
    )
    responses.add(
        responses.POST, "https://auth.example.com/token", json={"access_token": "new", "expires_in": 3600}
    )
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})
    responses.add(responses.GET, "https://api.example.com/ping", json={"ok": True})

    client = maki.MakiClient("https://api.example.com", auth=OAUTH2)
    client.get("/ping")
    client.get("/ping")

    api_calls = [c for c in responses.calls if "api.example.com" in c.request.url]
    assert api_calls[0].request.headers["Authorization"] == "Bearer old"
    assert api_calls[1].request.headers["Authorization"] == "Bearer new"


@responses.activate
def test_non_retryable_error_raises_http_error():
    responses.add(responses.GET, "https://api.example.com/missing", status=404)

    client = maki.MakiClient("https://api.example.com")
    with pytest.raises(requests.HTTPError):
        client.get("/missing")
    assert len(responses.calls) == 1
