"""Coverage extra runner."""

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from centraal_dataframework.config import TaskConfig
from centraal_dataframework.runner import Runner, get_runner, reset_runner


def test_cron_programable(runner):
    """Cron weekday 8am Bogotá."""
    # 2023-01-02 Monday 13:00 UTC = 08:00 Bogota
    dt = datetime(2023, 1, 2, 13, 0, tzinfo=timezone.utc)
    conf = TaskConfig(cron='0 8 * * 1-5', timezone='America/Bogota')
    assert runner.es_programable(conf, dt) is True


def test_next_runs_hours(runner):
    """next_runs para dias/horas."""
    runs = runner.next_runs('alpha', count=2)
    assert len(runs) == 2


def test_next_runs_cron(runner):
    """next_runs para cron."""
    runs = runner.next_runs('cron_task', count=2)
    assert len(runs) == 2


def test_get_runner_lazy(monkeypatch, tmp_path):
    """get_runner crea singleton."""
    reset_runner()
    monkeypatch.setenv('YAML_CONFIGURACION', str(tmp_path / 'missing.yaml'))
    from centraal_dataframework.settings import clear_settings_cache

    clear_settings_cache()
    r1 = get_runner()
    r2 = get_runner()
    assert r1 is r2
    reset_runner()


def test_dq_warn_only(runner, monkeypatch):
    """dq_warn_only no relanza."""
    from centraal_dataframework.excepciones import ErrorTareaCalidadDatos

    runner.conf.tareas['alpha'].dq_warn_only = True

    def boom(**kwargs):
        raise ErrorTareaCalidadDatos('fail', failed=['x'])

    runner.add_task(boom, 'alpha')
    monkeypatch.setattr(runner.notifier, 'notify_dq', lambda *a, **k: None)
    assert runner.run_task('alpha', notify=True) is None
