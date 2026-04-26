from __future__ import annotations

import os
import ssl
import urllib.request
from typing import Any, Callable

from flowgrid_app.runtime_logging import _runtime_log_event


DEFAULT_TIMEOUT_SECONDS = 20.0

_SSL_CONTEXT: ssl.SSLContext | None = None
_CA_FILE = ""
_CA_SOURCE = "system"


def _configure_ssl_context() -> ssl.SSLContext:
    global _CA_FILE, _CA_SOURCE
    try:
        import certifi  # type: ignore[import-not-found]

        cafile = str(certifi.where())
        _CA_FILE = cafile
        _CA_SOURCE = "certifi"
        os.environ.setdefault("SSL_CERT_FILE", cafile)
        os.environ.setdefault("REQUESTS_CA_BUNDLE", cafile)
        return ssl.create_default_context(cafile=cafile)
    except Exception as certifi_exc:
        _CA_FILE = ""
        _CA_SOURCE = "system"
        try:
            return ssl.create_default_context()
        except Exception as ssl_exc:
            _runtime_log_event(
                "network.ssl_context_failed",
                severity="error",
                summary="Failed creating a verified SSL context for Flowgrid network requests.",
                exc=ssl_exc,
                context={"certifi_error": f"{type(certifi_exc).__name__}: {certifi_exc}"},
            )
            raise


def _ssl_context() -> ssl.SSLContext:
    global _SSL_CONTEXT
    if _SSL_CONTEXT is None:
        _SSL_CONTEXT = _configure_ssl_context()
    return _SSL_CONTEXT


def _log_network_failure(
    log_event: Callable[..., Any] | None,
    event_key: str,
    *,
    url: str,
    timeout_seconds: float,
    requests_error: Exception | None,
    urllib_error: Exception,
) -> None:
    if log_event is None:
        return
    try:
        log_event(
            event_key,
            severity="warning",
            summary="Verified HTTPS request failed through requests and urllib.",
            exc=urllib_error,
            context={
                "url": str(url),
                "timeout_seconds": float(timeout_seconds),
                "transport_attempts": "requests, urllib",
                "ca_source": _CA_SOURCE,
                "ca_file": _CA_FILE,
                "requests_error": f"{type(requests_error).__name__}: {requests_error}"
                if requests_error is not None
                else "",
                "urllib_error": f"{type(urllib_error).__name__}: {urllib_error}",
            },
        )
    except Exception:
        return


def fetch_url_bytes(
    url: str,
    *,
    headers: dict[str, str] | None = None,
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
    log_event: Callable[..., Any] | None = _runtime_log_event,
    event_key: str = "network.request_failed",
) -> bytes:
    resolved_headers = headers or {}
    requests_error: Exception | None = None
    context = _ssl_context()

    try:
        import requests  # type: ignore[import-not-found]

        response = requests.get(
            url,
            headers=resolved_headers,
            timeout=timeout_seconds,
            verify=_CA_FILE or True,
        )
        response.raise_for_status()
        return bytes(response.content)
    except Exception as exc:
        requests_error = exc

    try:
        request = urllib.request.Request(url, headers=resolved_headers)
        with urllib.request.urlopen(request, context=context, timeout=timeout_seconds) as response:
            return response.read()
    except Exception as exc:
        _log_network_failure(
            log_event,
            event_key,
            url=url,
            timeout_seconds=timeout_seconds,
            requests_error=requests_error,
            urllib_error=exc,
        )
        raise


__all__ = ["DEFAULT_TIMEOUT_SECONDS", "fetch_url_bytes"]
