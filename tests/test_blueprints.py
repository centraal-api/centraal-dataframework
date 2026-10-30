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

    def capture_run(task_name, params=None, run_id=None, notify=True, raise_on_error=True, **kwargs):
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


def _failed_dq_task(runner, name):
    from centraal_dataframework.resources import DQResult
    from centraal_dataframework.tasks import task

    @task(name=name)
    def _t(ctx):
        return DQResult(success=False, failed=['e1'])


def test_dq_failure_notifies_first_attempt_and_is_not_retried(runner, monkeypatch):
    """Un fallo de calidad se notifica en el primer intento y no se reintenta (los datos no cambian)."""
    _failed_dq_task(runner, 'alpha')
    notified = []
    monkeypatch.setattr(runner.notifier, 'notify_dq', lambda *a, **k: notified.append(True))
    dependents = []
    monkeypatch.setattr(runner, 'dependents_of', lambda name: dependents.append(name) or [])

    msg = SimpleNamespace(get_body=lambda: b'alpha', dequeue_count=1)
    execute_tasks_inqueue(msg)  # type: ignore[arg-type]  # no lanza: el mensaje se consume

    assert notified == [True]
    assert dependents == []  # las dependientes no corren tras un fallo de calidad


def test_dependent_dq_failure_does_not_retry_parent(runner, monkeypatch):
    """Si falla la calidad de una dependiente, el mensaje del padre no se reintenta."""
    from centraal_dataframework.tasks import task

    @task(name='alpha')
    def alpha(ctx):
        return 'ok'

    _failed_dq_task(runner, 'gamma')
    monkeypatch.setattr(runner.notifier, 'notify_dq', lambda *a, **k: None)

    msg = SimpleNamespace(get_body=lambda: b'alpha', dequeue_count=1)
    execute_tasks_inqueue(msg)  # type: ignore[arg-type]
