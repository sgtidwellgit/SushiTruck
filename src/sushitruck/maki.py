"""maki — REST API client with auth, pagination, rate limiting, and retry."""

from __future__ import annotations

import base64
import time
from typing import Any

import pandas as pd
import requests

from . import gari, wasabi

_RETRYABLE_STATUS = (429, 500, 502, 503, 504)


def _dig(data: Any, dotted_key: str) -> Any:
    """Resolve a dot-notation key path against a nested dict, e.g. 'a.b' -> data['a']['b']."""

    value = data
    for part in dotted_key.split("."):
        if not isinstance(value, dict) or part not in value:
            raise KeyError(f"results_key '{dotted_key}' not found in response (failed at '{part}').")
        value = value[part]
    return value


class MakiClient:
    """A configured REST API client: base URL, auth, rate limiting, and retry."""

    def __init__(
        self,
        base_url: str,
        *,
        auth: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: int = 30,
        rate_limit: float | None = None,
        retries: int = 3,
        retry_on: tuple[int, ...] = _RETRYABLE_STATUS,
        session: requests.Session | None = None,
    ) -> None:
        """
        Configure a REST API client pointed at a base URL.

        Parameters
        ----------
        base_url
            Base URL that ``endpoint`` paths are joined against.
        auth
            Auth spec: ``{"type": "bearer", "token": ...}``,
            ``{"type": "basic", "username": ..., "password": ...}``,
            ``{"type": "api_key", "key": ..., "header": "X-API-Key"}`` (or
            ``"param": "api_key"`` instead of ``"header"``), or
            ``{"type": "oauth2", "token_url": ..., "client_id": ...,
            "client_secret": ...}``.
        headers
            Extra headers merged into every request.
        timeout
            Per-request timeout in seconds.
        rate_limit
            Maximum requests per second. ``None`` disables throttling.
        retries
            Total attempts per request (including the first), passed to
            :func:`sushitruck.gari.retry`.
        retry_on
            HTTP status codes that trigger a retry.
        session
            An existing ``requests.Session`` to reuse. A new one is created
            if omitted.
        """

        self.base_url = base_url.rstrip("/")
        self._auth = auth or {}
        self._extra_headers = headers or {}
        self.timeout = timeout
        self.session = session or requests.Session()
        self._limiter = gari.RateLimiter(rate_limit) if rate_limit else None
        self._retries = retries
        self._retry_on = retry_on
        self._oauth2_token: str | None = None
        self._oauth2_expires_at: float = 0.0

        if self._auth.get("type") not in (None, "bearer", "basic", "api_key", "oauth2"):
            raise ValueError(f"Unsupported auth type: {self._auth.get('type')!r}")

    def _build_url(self, endpoint: str) -> str:
        return f"{self.base_url}/{endpoint.lstrip('/')}"

    def _auth_headers_and_params(self) -> tuple[dict[str, str], dict[str, Any]]:
        auth_type = self._auth.get("type")
        headers: dict[str, str] = {}
        params: dict[str, Any] = {}

        if auth_type == "bearer":
            headers["Authorization"] = f"Bearer {self._auth['token']}"
        elif auth_type == "basic":
            raw = f"{self._auth['username']}:{self._auth['password']}".encode()
            headers["Authorization"] = f"Basic {base64.b64encode(raw).decode()}"
        elif auth_type == "api_key":
            if "param" in self._auth:
                params[self._auth["param"]] = self._auth["key"]
            else:
                headers[self._auth.get("header", "X-API-Key")] = self._auth["key"]
        elif auth_type == "oauth2":
            headers["Authorization"] = f"Bearer {self._get_oauth2_token()}"

        return headers, params

    def _get_oauth2_token(self) -> str:
        if self._oauth2_token is not None and time.monotonic() < self._oauth2_expires_at:
            return self._oauth2_token

        response = self.session.post(
            self._auth["token_url"],
            data={
                "grant_type": "client_credentials",
                "client_id": self._auth["client_id"],
                "client_secret": self._auth["client_secret"],
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()

        self._oauth2_token = payload["access_token"]
        self._oauth2_expires_at = time.monotonic() + payload.get("expires_in", 3600) - 30
        return self._oauth2_token

    def _request(self, method: str, endpoint: str, **kwargs: Any) -> requests.Response:
        auth_headers, auth_params = self._auth_headers_and_params()

        merged_headers = {**self._extra_headers, **auth_headers, **kwargs.pop("headers", {})}
        merged_params = {**auth_params, **(kwargs.pop("params", None) or {})}

        @gari.retry(max_attempts=self._retries, retry_on=self._retry_on)
        def do_request() -> requests.Response:
            if self._limiter is not None:
                self._limiter.sleep_until_ready()
            return self.session.request(
                method,
                self._build_url(endpoint),
                headers=merged_headers,
                params=merged_params or None,
                timeout=self.timeout,
                **kwargs,
            )

        response = do_request()
        response.raise_for_status()
        return response

    def get(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        paginate: bool = False,
        page_param: str = "page",
        page_size_param: str = "per_page",
        page_size: int = 100,
        results_key: str | None = None,
    ) -> Any:
        """
        Issue a GET request, optionally following pagination to collect all pages.

        Parameters
        ----------
        endpoint
            Path appended to ``base_url``.
        params
            Query parameters for the request.
        paginate
            Whether to keep requesting subsequent pages until exhausted.
            Detects, in order: a ``next_cursor``/``next_token`` field in the
            response body, a ``Link: <url>; rel="next"`` response header,
            then falls back to incrementing ``page_param``.
        page_param
            Query parameter name for the page number (page-number strategy).
        page_size_param
            Query parameter name for the page size.
        page_size
            Number of results requested per page.
        results_key
            Dot-notation key path to extract the results list from each
            page's JSON body (e.g. ``"data.items"``). If omitted, the whole
            parsed body is treated as the results for that page.

        Returns
        -------
        dict | list
            The parsed JSON body (single request), or a concatenated list
            of results across all pages (when ``paginate=True``).
        """

        if not paginate:
            response = self._request("GET", endpoint, params=params)
            return response.json()

        return self._get_paginated(
            endpoint, params or {}, page_param, page_size_param, page_size, results_key
        )

    def _get_paginated(
        self,
        endpoint: str,
        params: dict[str, Any],
        page_param: str,
        page_size_param: str,
        page_size: int,
        results_key: str | None,
    ) -> list[Any]:
        all_results: list[Any] = []
        page_params = {**params, page_size_param: page_size}
        page = 1
        next_url: str | None = None
        cursor_mode = False

        while True:
            if next_url is not None:
                response = self._request("GET", next_url)
            else:
                response = self._request("GET", endpoint, params={**page_params, page_param: page})
            body = response.json()

            page_results = _dig(body, results_key) if results_key else body
            if isinstance(page_results, list):
                all_results.extend(page_results)
            elif page_results:
                all_results.append(page_results)

            cursor = None
            if isinstance(body, dict):
                cursor = body.get("next_cursor") or body.get("next_token")

            if cursor:
                cursor_mode = True
                page_params["cursor"] = cursor
                next_url = None
                continue

            if cursor_mode:
                # A cursor sequence was in progress and this page had none: done.
                break

            link_next = response.links.get("next", {}).get("url") if response.links else None
            if link_next:
                next_url = link_next
                continue

            if not page_results:
                break

            next_url = None
            page += 1

        return all_results

    def post(
        self,
        endpoint: str,
        *,
        body: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Any:
        """
        Issue a POST request.

        Parameters
        ----------
        endpoint
            Path appended to ``base_url``.
        body
            Form-encoded request body.
        json
            JSON request body (mutually exclusive with ``body`` in intent;
            ``json`` takes precedence if both are given).

        Returns
        -------
        dict
            The parsed JSON response body.
        """

        response = self._request("POST", endpoint, data=body, json=json)
        return response.json()

    def fetch(
        self,
        endpoint: str,
        *,
        params: dict[str, Any] | None = None,
        paginate: bool = False,
        results_key: str | None = None,
        flatten: bool = True,
    ) -> pd.DataFrame:
        """
        GET an endpoint and return the results as a flat DataFrame.

        This is the highest-level read method: it calls :meth:`get`, then
        (by default) passes the results through :func:`sushitruck.wasabi.flatten`.

        Parameters
        ----------
        endpoint
            Path appended to ``base_url``.
        params
            Query parameters for the request.
        paginate
            Whether to follow pagination and merge all pages.
        results_key
            Dot-notation key path to extract the results list/object.
        flatten
            Whether to flatten nested JSON into columns via
            :func:`sushitruck.wasabi.flatten`. If ``False``, results are
            passed directly to ``pd.DataFrame``.

        Returns
        -------
        pd.DataFrame
            One row per result record.
        """

        data = self.get(
            endpoint,
            params=params,
            paginate=paginate,
            results_key=None if paginate else results_key,
        )

        if not paginate and results_key:
            data = _dig(data, results_key)

        if flatten:
            return wasabi.flatten(data)
        return pd.DataFrame(data if isinstance(data, list) else [data])
