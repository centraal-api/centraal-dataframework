"""Tests adicionales de blueprints."""

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from centraal_dataframework.blueprints import check_and_schedule_task, task_status
from centraal_dataframework.utils import build_queue_message


class FakeOut:
    def __init__(self):
        self.value = None

    def set(self, value):
        self.value = value


def test_check_and_schedule_empty(runner, monkeypatch):
    """Sin tareas programables avisa y no setea cola."""
    monkeypatch.setattr(runner, 'get_tareas_programables', lambda dt: [])
    out = FakeOut()
    timer = SimpleNamespace(past_due=True)
    check_and_schedule_task(timer, out)  # type: ignore[arg-type]
    assert out.value is None


def test_check_and_schedule_sets_messages(runner, monkeypatch):
    """Encola mensajes JSON."""
    monkeypatch.setattr(runner, 'get_tareas_programables', lambda dt: ['alpha'])
    out = FakeOut()
    timer = SimpleNamespace(past_due=False)
    check_and_schedule_task(timer, out)  # type: ignore[arg-type]
    assert out.value is not None
    assert 'alpha' in out.value[0]


def test_task_status_not_enabled(runner):
    """Historial deshabilitado -> 501."""
    runner.history = None
    req = SimpleNamespace(route_params={}, params={})
    resp = task_status(req)  # type: ignore[arg-type]
    assert resp.status_code == 501


def test_task_status_by_run_id(runner):
    """Consulta por run_id."""
    hist = MagicMock()
    hist.get_run.return_value = {'RowKey': 'r1', 'status': 'success'}
    runner.history = hist
    req = SimpleNamespace(route_params={'run_id': 'r1'}, params={})
    resp = task_status(req)  # type: ignore[arg-type]
    assert resp.status_code == 200


def test_task_status_missing(runner):
    """run_id no encontrado."""
    hist = MagicMock()
    hist.get_run.return_value = None
    runner.history = hist
    req = SimpleNamespace(route_params={'run_id': 'x'}, params={})
    resp = task_status(req)  # type: ignore[arg-type]
    assert resp.status_code == 404


def test_task_status_list(runner):
    """Lista runs."""
    hist = MagicMock()
    hist.list_runs.return_value = [{'RowKey': 'a'}]
    runner.history = hist
    req = SimpleNamespace(route_params={}, params={'task': 'alpha'})
    resp = task_status(req)  # type: ignore[arg-type]
    assert resp.status_code == 200
