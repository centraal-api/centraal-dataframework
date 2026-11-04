"""Tests del Runner."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from centraal_dataframework.config import TaskConfig
from centraal_dataframework.excepciones import ErrorEnTarea, ParametrosInvalidos, TareaDuplicada, TareaNoDefinida
from centraal_dataframework.runner import Runner


def test_es_programable_star(runner):
    """'*' / '*' siempre programable."""
    assert runner.es_programable(TaskConfig(dias='*', horas='*')) is True


def test_es_programable_hours_and_days(runner):
    """Lunes 8am Bogotá (UTC-5) = 13:00 UTC."""
    # 2023-01-02 was Monday (weekday 0). 13:00 UTC = 08:00 America/Bogota
    dt = datetime(2023, 1, 2, 13, 0, tzinfo=ZoneInfo('UTC'))
    conf = TaskConfig(dias='0,2,3', horas='8,12,16')
    assert runner.es_programable(conf, dt) is True


def test_es_programable_false(runner):
    """Fuera de ventana."""
    dt = datetime(2023, 1, 1, 18, 0, tzinfo=ZoneInfo('UTC'))  # domingo
    conf = TaskConfig(dias='1,2,3', horas='8,12,16')
    assert runner.es_programable(conf, dt) is False


def test_es_programable_spaces_in_yaml_values(runner):
    """Valores con espacios normalizados."""
    dt = datetime(2023, 1, 2, 13, 0, tzinfo=ZoneInfo('UTC'))
    conf = TaskConfig(dias='0, 2', horas=' 8 ,12')
    assert runner.es_programable(conf, dt) is True


def test_disabled_not_programable(runner):
    """enabled=false."""
    assert runner.es_programable(TaskConfig(dias='*', horas='*', enabled=False)) is False


def test_get_tareas_programables_skips_dependents(runner):
    """Dependientes no se programan por timer."""
    dt = datetime(2023, 1, 2, 13, 0, tzinfo=ZoneInfo('UTC'))
    tareas = runner.get_tareas_programables(dt)
    assert 'gamma' not in tareas
    assert 'alpha' in tareas


def test_get_emails_per_task(runner):
    """Emails por tarea tienen prioridad."""
    assert runner.get_emails('alpha') == ['alpha@example.com']
    assert runner.get_emails('beta') == ['ops@example.com']


def test_add_task_overwrite_warning(runner, caplog):
    """Sobrescribe solo si ya existía."""
    runner.add_task(lambda **k: None, 'alpha')
    runner.add_task(lambda **k: None, 'alpha')
    assert 'sobrescrita' in caplog.text.lower() or 'sobrescrita' in caplog.text


def test_add_task_duplicate_raises(settings, sample_conf):
    """overwrite_task=False lanza TareaDuplicada."""
    settings.overwrite_task = False
    r = Runner(settings=settings, conf=sample_conf)
    r.add_task(lambda **k: None, 'alpha')
    with pytest.raises(TareaDuplicada):
        r.add_task(lambda **k: None, 'alpha')


def test_run_task_success(runner):
    """Ejecuta tarea registrada."""
    calls = []

    def fake(**kwargs):
        calls.append(kwargs)
        return 'ok'

    runner.add_task(fake, 'alpha')
    assert runner.run_task('alpha', params={'x': 1}, notify=False) == 'ok'
    assert calls[0]['params']['x'] == 1


def test_run_task_undefined(runner):
    """Tarea no definida."""
    with pytest.raises(TareaNoDefinida):
        runner.run_task('nope')


def test_run_task_error_notifies(runner, monkeypatch):
    """Error notifica y lanza ErrorEnTarea."""
    notified = []

    def boom(**kwargs):
        raise RuntimeError('fail')

    monkeypatch.setattr(runner.notifier, 'notify_error', lambda *a, **k: notified.append(a))
    runner.add_task(boom, 'alpha')
    with pytest.raises(ErrorEnTarea):
        runner.run_task('alpha', notify=True)
    assert notified


def test_dependents_of(runner):
    """Mapa de dependientes."""
    assert runner.dependents_of('alpha') == ['gamma']


class _HistorialFalso:
    """Historial que guarda lo que el runner le pide registrar."""

    def __init__(self):
        self.inicios, self.exitos, self.fallos = [], [], []

    def record_start(self, task_name, run_id, params):
        self.inicios.append(task_name)

    def record_success(self, task_name, run_id, started, result=None):
        self.exitos.append(task_name)

    def record_failure(self, task_name, run_id, started, error, url=''):
        self.fallos.append((task_name, str(error)))


def test_parametros_invalidos_es_un_value_error():
    """La excepción hereda de ValueError y nombra la tarea y el motivo."""
    e = ParametrosInvalidos('alpha', "lote '24/09/2026' no es una fecha ISO")
    assert isinstance(e, ValueError)
    assert e.task_name == 'alpha'
    assert 'alpha' in str(e) and '24/09/2026' in str(e)


def test_run_task_parametros_invalidos_se_relanza_sin_envolver(runner, monkeypatch):
    """Un fallo definitivo de parámetros: historial 'failed' con el motivo, aviso una vez y la excepción tal cual."""
    avisos = []
    monkeypatch.setattr(runner.notifier, 'notify_error', lambda *a, **k: avisos.append(a))
    runner.history = _HistorialFalso()

    def mal_llamada(**kwargs):
        raise ParametrosInvalidos('alpha', 'lote no es ISO')

    runner.add_task(mal_llamada, 'alpha')
    with pytest.raises(ParametrosInvalidos):
        runner.run_task('alpha', notify=False, notify_dq=True)

    assert runner.history.fallos == [('alpha', "Parámetros inválidos en 'alpha': lote no es ISO")]
    assert len(avisos) == 1  # con la regla de los fallos definitivos (notify_dq), no con la de notify


def test_run_task_value_error_comun_sigue_como_error_en_tarea(runner, monkeypatch):
    """Regresión: un ValueError común no es definitivo; sale envuelto en ErrorEnTarea como hoy."""
    monkeypatch.setattr(runner.notifier, 'notify_error', lambda *a, **k: None)

    def boom(**kwargs):
        raise ValueError('algo')

    runner.add_task(boom, 'alpha')
    with pytest.raises(ErrorEnTarea):
        runner.run_task('alpha', notify=True)
