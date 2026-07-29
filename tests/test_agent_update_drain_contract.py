from __future__ import annotations

import signal
import threading
from types import SimpleNamespace
from urllib.parse import urlparse

import pytest

import api.routes as routes
import server


def test_handle_health_includes_drain_contract(monkeypatch):
    captured = {}
    monkeypatch.setattr(routes, 'SESSIONS', {'session-a': object()}, raising=False)
    monkeypatch.setattr(routes, '_streams_lock_health', lambda: {'status': 'ok', 'active_streams': 2})
    monkeypatch.setattr(routes, '_run_lifecycle_health', lambda: {'active_runs': 1, 'runs': [], 'last_run_finished_at': None})
    monkeypatch.setattr(routes, 'j', lambda _handler, payload, status=200, **_kwargs: captured.update(payload=payload, status=status) or True)

    handler = SimpleNamespace(
        server=SimpleNamespace(
            active_requests_inflight=3,
            draining=True,
            drain_signal_supported=True,
            accept_loop_requests_total=7,
            accept_loop_last_request_at=12.5,
        )
    )

    routes._handle_health(handler, SimpleNamespace(query=''))

    payload = captured['payload']
    assert captured['status'] == 200
    assert payload['sessions'] == 1
    assert payload['active_streams'] == 2
    assert payload['active_runs'] == 1
    assert payload['active_requests'] == 3
    assert payload['draining'] is True
    assert payload['drain_signal_supported'] is True
    assert payload['accept_loop']['requests_total'] == 7
    assert payload['accept_loop']['last_request_at'] == 12.5


def test_track_active_request_releases_on_exception():
    handler = server.Handler.__new__(server.Handler)
    handler.server = SimpleNamespace(active_requests_inflight=0, _active_requests_lock=threading.Lock())

    with pytest.raises(RuntimeError):
        with handler._track_active_request():
            assert handler.server.active_requests_inflight == 1
            raise RuntimeError('boom')

    assert handler.server.active_requests_inflight == 0


def test_reject_if_draining_rejects_non_health_and_allows_health(monkeypatch):
    captured = {}
    monkeypatch.setattr(server, 'j', lambda _handler, payload, status=200, **_kwargs: captured.update(payload=payload, status=status) or True)

    handler = server.Handler.__new__(server.Handler)
    handler.server = SimpleNamespace(draining=True)
    handler.close_connection = False

    assert handler._reject_if_draining(urlparse('/api/session')) is True
    assert handler.close_connection is True
    assert captured['status'] == 503
    assert 'draining' in captured['payload']['error'].lower()

    handler.close_connection = False
    assert handler._reject_if_draining(urlparse('/health')) is False


def test_install_lifecycle_signal_handlers_sets_drain_and_shutdown(monkeypatch):
    registered = {}
    shutdown_calls = []

    class InlineThread:
        def __init__(self, *, target, name=None, daemon=None):
            self._target = target

        def start(self):
            self._target()

    httpd = SimpleNamespace(draining=False, drain_started_at=0.0, drain_signal_supported=False, shutdown=lambda: shutdown_calls.append('shutdown'))

    monkeypatch.setattr(server.threading, 'Thread', InlineThread)
    monkeypatch.setattr(server.signal, 'signal', lambda signum, handler: registered.setdefault(signum, handler))

    server._install_lifecycle_signal_handlers(httpd)

    assert signal.SIGTERM in registered
    assert signal.SIGINT in registered
    if hasattr(signal, 'SIGUSR2'):
        assert signal.SIGUSR2 in registered
        registered[signal.SIGUSR2](signal.SIGUSR2, None)
        assert httpd.draining is True
        assert httpd.drain_started_at > 0
        assert httpd.drain_signal_supported is True

    registered[signal.SIGTERM](signal.SIGTERM, None)
    assert shutdown_calls == ['shutdown']
