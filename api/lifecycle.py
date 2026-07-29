"""WebUI request-drain accounting and lifecycle signal handlers."""

from __future__ import annotations

import logging
import signal
import threading
import time
from contextlib import contextmanager

from api.helpers import j

logger = logging.getLogger(__name__)


@contextmanager
def track_active_request(handler, *, count: bool = True):
    """Count an admitted request for drain coordination and always release it."""
    if not count:
        yield
        return
    server = getattr(handler, "server", None)
    lock = getattr(server, "_active_requests_lock", None)
    if server is not None and lock is not None:
        with lock:
            server.active_requests_inflight = int(
                getattr(server, "active_requests_inflight", 0) or 0
            ) + 1
    try:
        yield
    finally:
        if server is not None and lock is not None:
            with lock:
                server.active_requests_inflight = max(
                    0,
                    int(getattr(server, "active_requests_inflight", 0) or 0) - 1,
                )


def reject_if_draining(handler, parsed) -> bool:
    """Reject new non-health admission once the maintenance fence is active."""
    server = getattr(handler, "server", None)
    if not bool(getattr(server, "draining", False)):
        return False
    if getattr(parsed, "path", "") == "/health":
        return False
    handler.close_connection = True
    j(
        handler,
        {"error": "Hermes WebUI is draining for a protected Agent update. Retry shortly."},
        status=503,
    )
    return True


def install_lifecycle_signal_handlers(httpd, *, logger=logger) -> None:
    """Install orderly shutdown and maintenance-drain signal handlers."""
    shutdown_requested = threading.Event()
    httpd.drain_signal_supported = False

    def request_shutdown(_signum, _frame):
        if shutdown_requested.is_set():
            return
        shutdown_requested.set()
        threading.Thread(
            target=httpd.shutdown,
            name="webui-sigterm-shutdown",
            daemon=True,
        ).start()

    def request_drain(_signum, _frame):
        if getattr(httpd, "draining", False):
            return
        httpd.draining = True
        httpd.drain_started_at = time.time()
        logger.info("[drain] admission fence enabled for protected Agent update")

    try:
        signal.signal(signal.SIGTERM, request_shutdown)
        signal.signal(signal.SIGINT, request_shutdown)
        if hasattr(signal, "SIGUSR2"):
            signal.signal(signal.SIGUSR2, request_drain)
            httpd.drain_signal_supported = True
    except (ValueError, OSError):
        httpd.drain_signal_supported = False
        logger.debug("Could not install lifecycle signal handlers", exc_info=True)
