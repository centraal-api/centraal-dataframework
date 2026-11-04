"""Tests de blueprints Azure Functions."""

from __future__ import annotations

import json
from types import SimpleNamespace

from centraal_dataframework.blueprints import execute_tasks_inqueue, queue_task
from centraal_dataframework.excepciones import ErrorEnTarea


class FakeHttpRequest:
    """HttpRequest mínimo."""

    def __init__(self, body):
        """Guarda el cuerpo (o la excepción a lanzar)."""
        self._body = body

    def get_json(self):
        """Devuelve el cuerpo o lanza la excepción."""
        if isinstance(self._body, Exception):
            raise self._body
        return self._body


class FakeOut:
    """func.Out mínimo."""

    def __init__(self):
        """Sin valor."""
        self.value = None

    def set(self, value):
        """Guarda el valor."""
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
    """Encola tarea existente: siempre una lista de mensajes, aunque sea una sola tarea."""
    runner.add_task(lambda **k: None, 'alpha')
    req = FakeHttpRequest({'task_name': 'alpha', 'params': {'x': 1}})
    out = FakeOut()
    resp = queue_task(req, out)  # type: ignore[arg-type]
    assert resp.status_code == 200
    assert isinstance(out.value, list) and len(out.value) == 1
    assert 'alpha' in out.value[0]


def test_queue_task_varias_tareas_una_lista(runner):
    """Varias tareas -> una lista con un mensaje por tarea, en orden."""
    runner.add_task(lambda **k: None, 'alpha')
    runner.add_task(lambda **k: None, 'beta')
    out = FakeOut()
    resp = queue_task(FakeHttpRequest({'task_name': ['alpha', 'beta']}), out)  # type: ignore[arg-type]
    assert resp.status_code == 200
    from centraal_dataframework.utils import parse_queue_message

    assert [parse_queue_message(m)[0] for m in out.value] == ['alpha', 'beta']


def test_queue_task_anotacion_de_salida_es_un_tipo_concreto():
    """El worker de Functions rechaza Union en la anotación de un binding y no indexa ninguna función."""
    from typing import List, get_type_hints

    import azure.functions as func

    funcion = queue_task._function.get_user_function()  # el decorador deja un FunctionBuilder
    assert get_type_hints(funcion)['msg'] == func.Out[List[str]]


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


def _uses(*names):
    """Tarea mínima que declara recursos."""

    def fn(**kwargs):
        return None

    fn.uses = names
    return fn


def test_diag_ok_reports_version_python_and_drivers(runner, monkeypatch):
    """diag: 200 con versión, Python y drivers ODBC cuando no hay problemas."""
    import sys

    from centraal_dataframework import __version__
    from centraal_dataframework.blueprints import diagnostico

    monkeypatch.setitem(sys.modules, 'pyodbc', SimpleNamespace(drivers=lambda: ['ODBC Driver 18 for SQL Server']))
    runner.add_task(_uses(), 'alpha')
    resp = diagnostico(SimpleNamespace())  # type: ignore[arg-type]
    body = json.loads(resp.get_body())
    assert resp.status_code == 200
    assert body['version'] == __version__
    assert body['python'] == '.'.join(map(str, sys.version_info[:3]))
    assert body['odbc_drivers'] == ['ODBC Driver 18 for SQL Server']
    assert body['problemas'] == {}


def test_diag_503_when_a_task_resource_is_misconfigured(runner, monkeypatch):
    """diag: 503 si la imagen perdió el driver; sirve para una alerta de disponibilidad."""
    import sys

    from centraal_dataframework.blueprints import diagnostico

    monkeypatch.setenv('SYNAPSE_WORKSPACE', 'ws')
    monkeypatch.setitem(sys.modules, 'pyodbc', SimpleNamespace(drivers=lambda: []))
    runner.add_task(_uses('synapse'), 'carga')
    resp = diagnostico(SimpleNamespace())  # type: ignore[arg-type]
    body = json.loads(resp.get_body())
    assert resp.status_code == 503
    assert any('no está instalado' in p for p in body['problemas']['carga'])


def test_diag_without_pyodbc(runner, monkeypatch):
    """diag: sin pyodbc instalado, odbc_drivers es null."""
    import sys

    from centraal_dataframework.blueprints import diagnostico

    monkeypatch.setitem(sys.modules, 'pyodbc', None)
    resp = diagnostico(SimpleNamespace())  # type: ignore[arg-type]
    assert json.loads(resp.get_body())['odbc_drivers'] is None


def test_monitor_capacidad_logs_metrics_and_warns(monkeypatch, caplog):
    """El timer de capacidad deja la medición en el log y avisa con CAPACIDAD si hace falta escalar."""
    import logging

    from centraal_dataframework import blueprints, capacidad

    m = capacidad.Medicion(mensajes_en_cola=9, espera_max_min=90.0, memoria_pico_mb=100.0, memoria_limite_mb=3500.0)
    monkeypatch.setattr(capacidad, 'medir', lambda *a, **k: m)
    with caplog.at_level(logging.INFO, logger='centraal_dataframework.blueprints'):
        blueprints.monitor_capacidad(SimpleNamespace(past_due=False))  # type: ignore[arg-type]
    info = [r for r in caplog.records if r.levelno == logging.INFO and r.getMessage().startswith('capacidad ')]
    assert json.loads(info[0].getMessage()[len('capacidad ') :])['mensajes_en_cola'] == 9
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any(w.startswith('CAPACIDAD:') and 'instancia' in w for w in warnings)


def test_monitor_capacidad_quiet_when_one_instance_is_enough(monkeypatch, caplog):
    """Sin presión no hay avisos."""
    import logging

    from centraal_dataframework import blueprints, capacidad

    m = capacidad.Medicion(mensajes_en_cola=0, espera_max_min=0.0, memoria_pico_mb=100.0, memoria_limite_mb=3500.0)
    monkeypatch.setattr(capacidad, 'medir', lambda *a, **k: m)
    with caplog.at_level(logging.INFO, logger='centraal_dataframework.blueprints'):
        blueprints.monitor_capacidad(SimpleNamespace(past_due=False))  # type: ignore[arg-type]
    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_execute_logs_queue_wait(runner, caplog):
    """Cada ejecución registra cuánto esperó en la cola: es la señal de una instancia saturada."""
    import logging
    from datetime import datetime, timedelta, timezone

    runner.add_task(lambda **k: None, 'beta')  # sin dependientes en la config de ejemplo
    msg = SimpleNamespace(
        get_body=lambda: b'{"task": "beta"}',
        dequeue_count=1,
        insertion_time=datetime.now(timezone.utc) - timedelta(minutes=7),
    )
    with caplog.at_level(logging.INFO, logger='centraal_dataframework.blueprints'):
        execute_tasks_inqueue(msg)  # type: ignore[arg-type]
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith('tarea_capacidad ')]
    data = json.loads(lines[0][len('tarea_capacidad ') :])
    assert data['tarea'] == 'beta'
    assert 6.9 < data['espera_cola_min'] < 7.5
