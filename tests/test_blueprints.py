"""Tests de blueprints Azure Functions."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import azure.functions as func

from centraal_dataframework.blueprints import execute_tasks_inqueue, queue_task
from centraal_dataframework.excepciones import ErrorEnTarea


class FakeHttpRequest:
    """HttpRequest mínimo."""

    def __init__(self, body):
        self._body = body

    def get_json(self):
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class FakeOut:
    """func.Out mínimo."""

    def __init__(self):
        self.value = None

    def set(self, value):
        self.value = value


def test_queue_task_empty_list_400(runner):
    """Lista vacía -> 400."""
    req = FakeHttpRequest({'task_name': []})
    out = FakeOut()
    resp = queue_task(req, out)  # type: ignore[arg-type]
    assert resp.status_code == 400
    assert out.value is None


def test_queue_task_missing_task_404(runner):
    """Tarea inexistente -> 404."""
    req = FakeHttpRequest({'task_name': 'no_existe'})
    out = FakeOut()
    resp = queue_task(req, out)  # type: ignore[arg-type]
    assert resp.status_code == 404


def test_queue_task_ok(runner):
    """Encola tarea existente."""
    runner.add_task(lambda **k: None, 'alpha')
    req = FakeHttpRequest({'task_name': 'alpha', 'params': {'x': 1}})
    out = FakeOut()
    resp = queue_task(req, out)  # type: ignore[arg-type]
    assert resp.status_code == 200
    assert 'alpha' in out.value


def test_queue_task_non_object_400(runner):
    """Body lista -> 400."""
    req = FakeHttpRequest(['alpha'])
    out = FakeOut()
    resp = queue_task(req, out)  # type: ignore[arg-type]
    assert resp.status_code == 400


def test_execute_unknown_task_no_raise(runner):
    """Tarea desconocida no reintenta (no raise)."""
    msg = SimpleNamespace(
        get_body=lambda: b'unknown_task',
        dequeue_count=1,
    )
    execute_tasks_inqueue(msg)  # type: ignore[arg-type]


def test_execute_notifies_only_last_attempt(runner, monkeypatch):
    """notify=True solo en último intento."""
    calls = []

    def boom(**kwargs):
        raise RuntimeError('x')

    runner.add_task(boom, 'alpha')

    def capture_run(task_name, params=None, run_id=None, notify=True, raise_on_error=True):
        calls.append(notify)
        if raise_on_error:
            raise ErrorEnTarea(task_name)

    monkeypatch.setattr(runner, 'run_task', capture_run)

    msg = SimpleNamespace(get_body=lambda: b'alpha', dequeue_count=2)
    try:
        execute_tasks_inqueue(msg)  # type: ignore[arg-type]
    except ErrorEnTarea:
        pass
    assert calls == [False]

    calls.clear()
    msg = SimpleNamespace(get_body=lambda: b'alpha', dequeue_count=5)
    try:
        execute_tasks_inqueue(msg)  # type: ignore[arg-type]
    except ErrorEnTarea:
        pass
    assert calls == [True]
