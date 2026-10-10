"""Tests de decoradores y TaskContext."""

import logging

from centraal_dataframework.excepciones import ErrorTareaCalidadDatos
from centraal_dataframework.resources import DQResult
from centraal_dataframework.runner import reset_runner, set_runner
from centraal_dataframework.tasks import _LOGGER_CONFIGURED, TaskContext, task, task_dq


def test_task_decorator_registers_and_injects_context(runner, monkeypatch):
    """@task inyecta TaskContext."""
    monkeypatch.setattr('centraal_dataframework.tasks.get_datalake', lambda: 'dl')

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        assert ctx.datalake == 'dl'
        assert ctx.name == 'alpha'
        assert ctx.run_id
        ctx.logger.info('hello')
        return ctx.params.get('n')

    assert runner.get_task('alpha') is not None
    assert runner.run_task('alpha', params={'n': 7}, notify=False) == 7


def test_task_without_parens(runner, monkeypatch):
    """@task sin paréntesis."""
    monkeypatch.setattr('centraal_dataframework.tasks.get_datalake', lambda: 'dl')

    @task
    def beta(ctx: TaskContext):
        return 'ok'

    # beta nombre choca con conf; task_must_be_in_conf False en fixture
    assert runner.run_task('beta', notify=False) == 'ok'


def test_logger_handlers_not_duplicated(runner, monkeypatch):
    """No acumula StreamHandlers."""
    monkeypatch.setattr('centraal_dataframework.tasks.get_datalake', lambda: 'dl')
    _LOGGER_CONFIGURED.discard('alpha')

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        return True

    runner.run_task('alpha', notify=False)
    runner.run_task('alpha', notify=False)
    logger = logging.getLogger('centraal_dataframework.task.alpha')
    # Solo contamos handlers nuestros (StreamHandler "puros", no LogCapture de pytest)
    ours = [
        h
        for h in logger.handlers
        if type(h) is logging.StreamHandler and not h.__class__.__name__.startswith('LogCapture')
    ]
    assert len(ours) == 1


def test_task_dq_fail_on_error(runner, monkeypatch):
    """fail_on_error lanza ErrorTareaCalidadDatos."""
    monkeypatch.setattr('centraal_dataframework.tasks.get_datalake', lambda: 'dl')
    monkeypatch.setattr('centraal_dataframework.tasks.get_context', lambda: object())

    class FakeGX:
        def __init__(self, *a, **k):
            pass

    monkeypatch.setattr('centraal_dataframework.tasks.GreatExpectationsToolKit', FakeGX)

    @task_dq(name='alpha', fail_on_error=True)
    def alpha(ctx: TaskContext):
        return DQResult(success=False, url='http://x', failed=['e1'])

    notified = []
    monkeypatch.setattr(runner.notifier, 'notify_dq', lambda *a, **k: notified.append(True))
    try:
        runner.run_task('alpha', notify=True)
        assert False, 'should raise'
    except ErrorTareaCalidadDatos:
        pass
    assert notified


class _FakeSynapse:
    """SynapseClient falso."""

    def __init__(self, database=None):
        """Guarda la base."""
        self.database = database

    def for_database(self, database):
        """Copia con otra base."""
        return _FakeSynapse(database)


def test_context_is_lazy_when_resources_unused(runner, monkeypatch):
    """Si la tarea no usa datalake ni synapse, no se crean (tarea sin storage configurado)."""
    calls = []

    def no_storage():
        calls.append('dl')
        raise AssertionError('no debe crearse')

    monkeypatch.setattr('centraal_dataframework.tasks.get_datalake', no_storage)
    monkeypatch.setattr('centraal_dataframework.tasks.get_synapse', lambda: calls.append('syn'), raising=False)

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        return 'ok'

    assert runner.run_task('alpha', notify=False) == 'ok'
    assert calls == []


def test_context_synapse_created_once_and_synapse_for(runner, monkeypatch):
    """ctx.synapse se crea una vez; synapse_for cambia la base."""
    created = []

    def factory():
        created.append(1)
        return _FakeSynapse('default')

    monkeypatch.setattr('centraal_dataframework.tasks.get_synapse', factory, raising=False)

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        assert ctx.synapse is ctx.synapse
        return ctx.synapse_for('silver').database

    assert runner.run_task('alpha', notify=False) == 'silver'
    assert created == [1]
