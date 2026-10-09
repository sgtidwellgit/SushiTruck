"""maki — REST API client with auth, pagination, rate limiting, and retry."""

from __future__ import annotations

import base64
import time
from typing import Any

import pandas as pd
import requests

from . import gari, wasabi

_RETRYABLE_STATUS = (429, 500, 502, 503, 504)
_PAGINATION_STRATEGIES = ("auto", "page", "offset", "cursor", "link")


def _dig(data: Any, dotted_key: str) -> Any:
    """Resolve a dot-notation key path against a nested dict, e.g. 'a.b' -> data['a']['b']."""

    value = data
    for part in dotted_key.split("."):
        if not isinstance(value, dict) or part not in value:
            raise KeyError(f"results_key '{dotted_key}' not found in response (failed at '{part}').")
        value = value[part]
    return value


def _next_cursor(body: Any) -> Any:
    """Return the ``next_cursor`` / ``next_token`` value from a response body, if any."""

    if isinstance(body, dict):
        return body.get("next_cursor") or body.get("next_token")
    return None


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
        if endpoint.startswith(("http://", "https://")):
            # Absolute URLs (e.g. a Link-header "next" page) are used as-is.
            return endpoint
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
        pagination: str = "auto",
        page_param: str = "page",
        page_size_param: str | None = None,
        page_size: int = 100,
        offset_param: str = "offset",
        cursor_param: str = "cursor",
        results_key: str | None = None,
    ) -> Any:
        """
        Issue a GET request, optionally following pagination to collect all pages.

        Parameters
        ----------
        endpoint
            Path appended to ``base_url``, or an absolute ``http(s)://`` URL.
        params
            Query parameters for the request.
        paginate
            Whether to keep requesting subsequent pages until exhausted.
        pagination
            Pagination strategy used when ``paginate=True``:

            - ``"page"`` -- increment ``page_param`` (1, 2, 3, ...) until a
              page returns no results.
            - ``"offset"`` -- advance ``offset_param`` by ``page_size``
              (0, 100, 200, ...) until a page returns fewer than
              ``page_size`` results.
            - ``"cursor"`` -- send the ``next_cursor`` / ``next_token`` value
              from each response body back as ``cursor_param`` until a
              response no longer carries one.
            - ``"link"`` -- follow the ``Link: <url>; rel="next"`` response
              header until it is absent.
            - ``"auto"`` (default) -- start with page numbers, and switch to
              cursor or Link-header following as soon as a response
              carries a cursor or a ``rel="next"`` link.
        page_param
            Query parameter name for the page number (``"page"`` strategy).
        page_size_param
            Query parameter name for the page size. Defaults to ``"limit"``
            for the ``"offset"`` strategy and ``"per_page"`` otherwise.
        page_size
            Number of results requested per page.
        offset_param
            Query parameter name for the row offset (``"offset"`` strategy).
        cursor_param
            Query parameter name the cursor is sent back as (``"cursor"``
            strategy, or ``"auto"`` once a cursor is seen).
        results_key
            Dot-notation key path to extract the results from the JSON body
            (e.g. ``"data.items"``). Applied to every page when paginating.
            If omitted, the whole parsed body is the result.

        Returns
        -------
        dict | list
            The parsed JSON body (or the value at ``results_key``) for a
            single request, or a concatenated list of results across all
            pages when ``paginate=True``.

        Raises
        ------
        ValueError
            If ``pagination`` is not a supported strategy.
        KeyError
            If ``results_key`` is not present in a response body.
        """

        if not paginate:
            body = self._request("GET", endpoint, params=params).json()
            return _dig(body, results_key) if results_key else body

        if pagination not in _PAGINATION_STRATEGIES:
            raise ValueError(
                f"pagination must be one of {_PAGINATION_STRATEGIES}, got {pagination!r}"
            )

        return self._get_paginated(
            endpoint,
            params or {},
            strategy=pagination,
            page_param=page_param,
            page_size_param=page_size_param,
            page_size=page_size,
            offset_param=offset_param,
            cursor_param=cursor_param,
            results_key=results_key,
        )

    def _get_paginated(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        strategy: str,
        page_param: str,
        page_size_param: str | None,
        page_size: int,
        offset_param: str,
        cursor_param: str,
        results_key: str | None,
    ) -> list[Any]:
        size_param = page_size_param or ("limit" if strategy == "offset" else "per_page")
        base_params = {**params, size_param: page_size}

        all_results: list[Any] = []
        mode = strategy
        page = 1
        offset = 0
        cursor: Any = None
        next_url: str | None = None

        while True:
            if next_url is not None:
                response = self._request("GET", next_url)
            else:
                request_params = dict(base_params)
                if mode in ("auto", "page"):
                    request_params[page_param] = page
                elif mode == "offset":
                    request_params[offset_param] = offset
                elif mode == "cursor" and cursor is not None:
                    request_params[cursor_param] = cursor
                response = self._request("GET", endpoint, params=request_params)
            body = response.json()

            page_results = _dig(body, results_key) if results_key else body
            if isinstance(page_results, list):
                all_results.extend(page_results)
                count = len(page_results)
            elif page_results:
                all_results.append(page_results)
                count = 1
            else:
                count = 0

            if mode in ("auto", "cursor"):
                cursor = _next_cursor(body)
                if cursor:
                    mode = "cursor"
                    continue
                if mode == "cursor":
                    break

            if mode in ("auto", "link"):
                link_next = response.links.get("next", {}).get("url") if response.links else None
                if link_next:
                    mode = "link"
                    next_url = link_next
                    continue
                if mode == "link":
                    break

            if count == 0:
                break
            if mode == "offset":
                if count < page_size:
                    break
                offset += page_size
            else:
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
        pagination: str = "auto",
        page_param: str = "page",
        page_size_param: str | None = None,
        page_size: int = 100,
        offset_param: str = "offset",
        cursor_param: str = "cursor",
        results_key: str | None = None,
        flatten: bool = True,
    ) -> pd.DataFrame:
        """
        GET an endpoint and return the results as a flat DataFrame.

        This is the highest-level read method: it calls :meth:`get`, then
        (by default) passes the results through :func:`sushitruck.wasabi.flatten`.
        All pagination parameters have the same meaning as in :meth:`get`.

        Parameters
        ----------
        endpoint
            Path appended to ``base_url``.
        params
            Query parameters for the request.
        paginate
            Whether to follow pagination and merge all pages.
        pagination, page_param, page_size_param, page_size, offset_param, cursor_param
            Pagination controls; see :meth:`get`.
        results_key
            Dot-notation key path to extract the results list/object from
            each response body.
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
            pagination=pagination,
            page_param=page_param,
            page_size_param=page_size_param,
            page_size=page_size,
            offset_param=offset_param,
            cursor_param=cursor_param,
            results_key=results_key,
        )

        if flatten:
            return wasabi.flatten(data)
        return pd.DataFrame(data if isinstance(data, list) else [data])
