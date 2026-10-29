"""Tests de decoradores y TaskContext."""

import logging
from datetime import date, datetime, timezone

import pytest

from centraal_dataframework import registry
from centraal_dataframework.config import DQSilence
from centraal_dataframework.excepciones import (
    ErrorEnTarea,
    ErrorTareaCalidadDatos,
    RecursoDesconocido,
    RecursoNoConfigurado,
    RecursoNoDeclarado,
)
from centraal_dataframework.resources import DQResult
from centraal_dataframework.tasks import _LOGGER_CONFIGURED, TaskContext, task


@pytest.fixture
def fake_platform(monkeypatch):
    """Reemplaza una plataforma del registro por una factory falsa que registra sus llamadas."""

    def _install(name, value=None, check=None):
        calls = []

        def factory(settings, ctx):
            calls.append((settings.instance, ctx.name))
            return value if value is not None else f"{name}:{settings.instance or 'default'}"

        monkeypatch.setitem(registry._REGISTRY, name, registry.Resource(name, factory, check))
        return calls

    return _install


def test_task_decorator_registers_and_injects_context(runner, fake_platform):
    """@task inyecta TaskContext con los recursos declarados."""
    fake_platform('datalake', value='dl')

    @task(name='alpha', uses=['datalake'])
    def alpha(ctx: TaskContext):
        assert ctx.datalake == 'dl'
        assert ctx.name == 'alpha'
        assert ctx.run_id
        ctx.logger.info('hello')
        return ctx.params.get('n')

    assert runner.get_task('alpha').uses == ('datalake',)
    assert runner.run_task('alpha', params={'n': 7}, notify=False) == 7


def test_task_without_parens(runner):
    """@task sin paréntesis no declara recursos."""

    @task
    def beta(ctx: TaskContext):
        return 'ok'

    assert runner.run_task('beta', notify=False) == 'ok'
    assert runner.get_task('beta').uses == ()


def test_logger_handlers_not_duplicated(runner):
    """No acumula StreamHandlers."""
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


def test_failed_dq_result_raises_and_notifies(runner, fake_platform, monkeypatch):
    """Una tarea que devuelve un DQResult fallido termina en ErrorTareaCalidadDatos y notifica DQ."""
    fake_platform('gx')

    @task(name='alpha', uses=['gx'])
    def alpha(ctx: TaskContext):
        assert ctx.gx == 'gx:default'
        return DQResult(success=False, url='http://x', failed=['e1'])

    notified = []
    monkeypatch.setattr(runner.notifier, 'notify_dq', lambda *a, **k: notified.append(True))
    with pytest.raises(ErrorTareaCalidadDatos):
        runner.run_task('alpha', notify=True)
    assert notified


def test_successful_dq_result_is_returned(runner):
    """Un DQResult exitoso se devuelve sin error."""

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        return DQResult(success=True)

    assert runner.run_task('alpha', notify=False).success


def test_undeclared_resource_fails(runner, fake_platform):
    """Usar un recurso no declarado falla aunque la plataforma exista y esté configurada."""
    calls = fake_platform('synapse')
    fake_platform('datalake')

    @task(name='alpha', uses=['datalake'])
    def alpha(ctx: TaskContext):
        return ctx.synapse

    with pytest.raises(ErrorEnTarea) as exc:
        runner.run_task('alpha', notify=False)
    assert isinstance(exc.value.__cause__, RecursoNoDeclarado)
    assert calls == []


def test_named_instance_requires_its_own_declaration(fake_platform):
    """Declarar 'datalake:curado' no autoriza el datalake por defecto, y viceversa."""
    fake_platform('datalake')
    ctx = TaskContext(name='t', run_id='r', logger=logging.getLogger('t'), uses=('datalake:curado',))
    assert ctx.resource('datalake', 'CURADO') == 'datalake:curado'
    assert ctx.resource('datalake:curado') is ctx.resource('datalake', 'curado')
    with pytest.raises(RecursoNoDeclarado):
        ctx.datalake


def test_unknown_platform_fails_at_decoration(runner):
    """Declarar una plataforma no registrada falla al decorar, no al ejecutar."""
    with pytest.raises(RecursoDesconocido):

        @task(name='alpha', uses=['no_existe'])
        def alpha(ctx: TaskContext):
            return None

    assert runner.get_task('alpha') is None


def test_invalid_use_fails_at_decoration(runner):
    """Un uso mal escrito falla al decorar."""
    with pytest.raises(ValueError):

        @task(name='alpha', uses=['datalake:con espacio'])
        def alpha(ctx: TaskContext):
            return None


def test_declared_resource_not_configured_fails_before_running(runner, fake_platform):
    """Si un recurso declarado no está configurado, la tarea no se ejecuta."""
    fake_platform('synapse', check=lambda s: ['falta SYNAPSE_WORKSPACE'])
    ran = []

    @task(name='alpha', uses=['synapse'])
    def alpha(ctx: TaskContext):
        ran.append(True)

    with pytest.raises(ErrorEnTarea) as exc:
        runner.run_task('alpha', notify=False)
    assert isinstance(exc.value.__cause__, RecursoNoConfigurado)
    assert 'synapse: falta SYNAPSE_WORKSPACE' in str(exc.value.__cause__)
    assert ran == []


def test_runner_check_resources_lists_only_tasks_with_problems(runner, fake_platform):
    """check_resources revisa todas las tareas sin ejecutarlas."""
    fake_platform('synapse', check=lambda s: ['falta workspace'])
    fake_platform('datalake', check=lambda s: [])

    @task(name='alpha', uses=['synapse'])
    def alpha(ctx: TaskContext):
        return None

    @task(name='beta', uses=['datalake'])
    def beta(ctx: TaskContext):
        return None

    assert runner.check_resources() == {'alpha': ['synapse: falta workspace']}


def test_context_is_lazy(runner, fake_platform):
    """Declarar no crea el recurso: se crea al primer uso y una sola vez."""
    calls = fake_platform('synapse')

    @task(name='alpha', uses=['synapse'])
    def alpha(ctx: TaskContext):
        assert calls == []
        assert ctx.synapse is ctx.synapse
        return len(calls)

    assert runner.run_task('alpha', notify=False) == 1


class _FakeSynapse:
    """SynapseClient falso."""

    def __init__(self, database=None):
        """Guarda la base."""
        self.database = database

    def for_database(self, database):
        """Copia con otra base."""
        return _FakeSynapse(database)


def test_synapse_for(runner, fake_platform):
    """synapse_for cambia la base sobre el Synapse por defecto."""
    fake_platform('synapse', value=_FakeSynapse('default'))

    @task(name='alpha', uses=['synapse'])
    def alpha(ctx: TaskContext):
        return ctx.synapse_for('silver').database

    assert runner.run_task('alpha', notify=False) == 'silver'


def test_injected_resources_still_require_declaration():
    """Recursos inyectados (tests) también respetan la declaración."""
    ctx = TaskContext(
        name='t', run_id='r', logger=logging.getLogger('t'), uses=('datalake',), resources={'datalake': 'dl'}
    )
    assert ctx.datalake == 'dl'
    with pytest.raises(RecursoNoDeclarado):
        ctx.synapse


def test_resource_rejects_invalid_instance(fake_platform):
    """Una instancia vacía o inválida no cae en la conexión por defecto."""
    fake_platform('datalake')
    ctx = TaskContext(name='t', run_id='r', logger=logging.getLogger('t'), uses=('datalake',))
    for bad in ['', 'a-b', 'a:b']:
        with pytest.raises(ValueError):
            ctx.resource('datalake', bad)
    assert ctx.resources == {}


def test_runner_settings_reach_the_context(runner, monkeypatch):
    """La verificación previa y la factory usan las mismas Settings (las del runner)."""
    seen = []
    monkeypatch.setitem(
        registry._REGISTRY,
        'datalake',
        registry.Resource('datalake', lambda s, ctx: seen.append(s) or 'dl', lambda s: []),
    )

    @task(name='alpha', uses=['datalake'])
    def alpha(ctx: TaskContext):
        return ctx.datalake

    assert runner.run_task('alpha', notify=False) == 'dl'
    assert seen == [runner.settings]


def test_direct_call_returns_failed_dq_result(runner):
    """Llamar la tarea directo (notebook) devuelve el DQResult fallido: la regla la aplica el runner."""
    failed = DQResult(success=False, url='http://x', failed=['e1'])

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        return failed

    assert alpha() is failed


def _silence(sample_conf, hasta):
    sample_conf.tareas['alpha'].silenciar_dq = DQSilence(hasta=hasta, motivo='ticket #123')


def test_silenced_dq_does_nothing(runner, sample_conf, monkeypatch, caplog):
    """Con el silencio vigente: no lanza, no notifica y devuelve el DQResult; queda en el log."""
    _silence(sample_conf, date(2999, 1, 1))
    failed = DQResult(success=False, url='http://x', failed=['e1'])

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        return failed

    notified = []
    monkeypatch.setattr(runner.notifier, 'notify_dq', lambda *a, **k: notified.append(True))
    with caplog.at_level(logging.WARNING, logger='centraal_dataframework.runner'):
        assert runner.run_task('alpha', notify=True) is failed
    assert notified == []
    assert 'DQ silenciada hasta 2999-01-01 (ticket #123)' in caplog.text


def test_expired_silence_raises_and_notifies(runner, sample_conf, monkeypatch):
    """Al vencer el silencio, la alarma vuelve sola."""
    _silence(sample_conf, date(2000, 1, 1))

    @task(name='alpha')
    def alpha(ctx: TaskContext):
        return DQResult(success=False, failed=['e1'])

    notified = []
    monkeypatch.setattr(runner.notifier, 'notify_dq', lambda *a, **k: notified.append(True))
    with pytest.raises(ErrorTareaCalidadDatos):
        runner.run_task('alpha', notify=True)
    assert notified


def test_silence_includes_the_whole_day_in_configured_timezone(runner, sample_conf):
    """El silencio cubre todo el día de hasta en TIMEZONE (America/Bogota, UTC-5)."""
    _silence(sample_conf, date(2026, 11, 15))
    # 2026-11-16 04:59 UTC = 2026-11-15 23:59 en Bogotá: vigente.
    assert runner.dq_silence('alpha', datetime(2026, 11, 16, 4, 59, tzinfo=timezone.utc)) is not None
    # 2026-11-16 05:00 UTC = 2026-11-16 00:00 en Bogotá: vencido.
    assert runner.dq_silence('alpha', datetime(2026, 11, 16, 5, 0, tzinfo=timezone.utc)) is None
    assert runner.dq_silence('beta') is None
    assert runner.dq_silence('no_en_yaml') is None
