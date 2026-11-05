"""Tests del registro de plataformas y de las conexiones con nombre."""

import pytest

from centraal_dataframework import registry
from centraal_dataframework.excepciones import RecursoDesconocido
from centraal_dataframework.registry import check_uses, normalize_uses, parse_use, register_resource
from centraal_dataframework.settings import Settings, settings_for_instance


def test_core_platforms_registered():
    """datalake, synapse y gx vienen registradas."""
    assert {'datalake', 'synapse', 'gx'} <= set(registry.registered_resources())


def test_register_custom_platform(monkeypatch):
    """Un proyecto cliente registra su plataforma con la misma API del core."""
    monkeypatch.setattr(registry, '_REGISTRY', dict(registry._REGISTRY))
    register_resource('mi_api', lambda settings, ctx: 'cliente', check=lambda s: [])
    assert normalize_uses(['mi_api', 'mi_api:Otra']) == ('mi_api', 'mi_api:otra')


def test_register_twice_requires_replace(monkeypatch):
    """No se reemplaza una plataforma por accidente."""
    monkeypatch.setattr(registry, '_REGISTRY', dict(registry._REGISTRY))
    with pytest.raises(ValueError):
        register_resource('datalake', lambda s, c: None)
    register_resource('datalake', lambda s, c: None, replace=True)


@pytest.mark.parametrize('name', ['', '1abc', 'con-guion', 'a:b'])
def test_register_invalid_name(name):
    """Nombres inválidos se rechazan."""
    with pytest.raises(ValueError):
        register_resource(name, lambda s, c: None)


def test_parse_use():
    """Separa plataforma e instancia (en minúsculas)."""
    assert parse_use('synapse') == ('synapse', None)
    assert parse_use('synapse:GOLD') == ('synapse', 'gold')
    for bad in ['synapse:', ':x', 'synapse:a-b', 'syn apse']:
        with pytest.raises(ValueError):
            parse_use(bad)


def test_normalize_uses_unknown_and_duplicates():
    """Quita duplicados y falla con plataformas no registradas."""
    assert normalize_uses(['synapse', 'synapse', 'datalake']) == ('synapse', 'datalake')
    assert normalize_uses('gx') == ('gx',)
    with pytest.raises(RecursoDesconocido):
        normalize_uses(['bigquery'])


def test_named_instance_reads_only_suffixed_variables(monkeypatch):
    """Una conexión con nombre no hereda las variables globales."""
    monkeypatch.setenv('SYNAPSE_WORKSPACE', 'global')
    monkeypatch.setenv('SYNAPSE_DATABASE', 'master')
    monkeypatch.setenv('SYNAPSE_WORKSPACE__GOLD', 'ws-gold')
    s = settings_for_instance('gold')
    assert s.instance == 'gold'
    assert s.synapse_workspace == 'ws-gold'
    assert s.synapse_database is None
    assert s.env_name('SYNAPSE_DATABASE') == 'SYNAPSE_DATABASE__GOLD'


def test_named_datalake_does_not_fall_back_to_webjobs_storage(monkeypatch):
    """datalake:curado no apunta en silencio al storage de la Function."""
    monkeypatch.setenv('AzureWebJobsStorage', 'AccountName=fn;AccountKey=k')
    assert settings_for_instance('curado').resolve_storage_connection_string() is None
    problems = check_uses(['datalake:curado'])
    assert problems == [
        'datalake:curado: Configure AZURE_STORAGE_CONNECTION_STRING__CURADO o STORAGE_ACCOUNT_NAME__CURADO.'
    ]


def test_check_uses_core_platforms(monkeypatch):
    """Las verificaciones del core no se conectan y nombran la variable que falta."""
    monkeypatch.delenv('AzureWebJobsStorage', raising=False)
    monkeypatch.setattr(registry, '_REGISTRY', dict(registry._REGISTRY))
    base = Settings(synapse_workspace=None, synapse_server=None, storage_account_name='acc')
    problems = check_uses(['datalake', 'synapse', 'gx'], base)
    assert not any(p.startswith('datalake') for p in problems)
    assert any('SYNAPSE_WORKSPACE' in p for p in problems)
    assert not any(p.startswith('gx') for p in problems)


def test_check_uses_reports_check_exceptions(monkeypatch):
    """Si la verificación misma falla, se reporta como problema."""
    monkeypatch.setattr(registry, '_REGISTRY', dict(registry._REGISTRY))

    def boom(settings):
        raise RuntimeError('mal')

    register_resource('rara', lambda s, c: None, check=boom)
    assert check_uses(['rara']) == ['rara: RuntimeError: mal']


def test_named_instance_uses_its_storage_connection_setting(monkeypatch):
    """STORAGE_CONNECTION_SETTING__<NOMBRE> apunta a un app setting propio de la conexión."""
    monkeypatch.setenv('AzureWebJobsStorage', 'AccountName=fn;AccountKey=k')
    monkeypatch.setenv('STORAGE_CONNECTION_SETTING__CURADO', 'CONN_CURADO')
    monkeypatch.setenv('CONN_CURADO', 'AccountName=curado;AccountKey=k')
    assert settings_for_instance('curado').resolve_storage_connection_string() == 'AccountName=curado;AccountKey=k'
    assert check_uses(['datalake:curado']) == []


def test_named_instance_inherits_operational_settings(monkeypatch):
    """Hereda lo que no identifica la conexión (contenedor, timeout); el sufijo lo sobrescribe."""
    monkeypatch.setenv('CONTENEDOR_VALIDACIONES', 'dq-global')
    monkeypatch.setenv('SYNAPSE_QUERY_TIMEOUT', '300')
    monkeypatch.setenv('SYNAPSE_QUERY_TIMEOUT__GOLD', '600')
    monkeypatch.setenv('SYNAPSE_WORKSPACE', 'global')
    s = settings_for_instance('gold')
    assert s.validations_container == 'dq-global'
    assert s.synapse_query_timeout == 600
    assert s.synapse_workspace is None


def test_named_instance_inherits_from_injected_base():
    """Con Settings inyectadas (runner), la conexión con nombre hereda de ellas, no de las globales."""
    base = Settings(validations_container='dq-runner', synapse_workspace='ws-runner')
    s = settings_for_instance('gold', base)
    assert s.validations_container == 'dq-runner'
    assert s.synapse_workspace is None


def test_named_instance_settings_are_cached(monkeypatch):
    """Se construyen una vez por nombre; clear_settings_cache las invalida."""
    from centraal_dataframework.settings import clear_settings_cache

    first = settings_for_instance('gold')
    assert settings_for_instance('GOLD') is first
    clear_settings_cache()
    assert settings_for_instance('gold') is not first


@pytest.mark.parametrize('bad', ['synapse\n', 'synapse:gold\n', 'synapse:'])
def test_parse_use_rejects_trailing_newline(bad):
    """No acepta un salto de línea final."""
    with pytest.raises(ValueError):
        parse_use(bad)


def test_gx_check_reports_missing_package(monkeypatch):
    """La plataforma gx se reporta sin configurar si great_expectations no está instalado."""
    import importlib.util

    real = importlib.util.find_spec
    monkeypatch.setattr(importlib.util, 'find_spec', lambda n, *a: None if n == 'great_expectations' else real(n, *a))
    assert check_uses(['gx']) == ['gx: Falta great_expectations: instale great-expectations>=1.3.']


def _fake_pyodbc(monkeypatch, drivers):
    """Instala un pyodbc falso que lista los drivers dados."""
    import sys
    from types import SimpleNamespace

    monkeypatch.setitem(sys.modules, 'pyodbc', SimpleNamespace(drivers=lambda: list(drivers)))


def test_synapse_check_reports_missing_odbc_driver(monkeypatch):
    """Si el driver configurado no está instalado (p. ej. en la Function), el check lo dice."""
    _fake_pyodbc(monkeypatch, ['ODBC Driver 17 for SQL Server'])
    problems = registry._synapse_check(Settings(synapse_workspace='ws'))
    assert len(problems) == 1
    assert "'ODBC Driver 18 for SQL Server' no está instalado" in problems[0]
    assert 'ODBC Driver 17 for SQL Server' in problems[0]
    assert 'SYNAPSE_ODBC_DRIVER' in problems[0]


def test_synapse_check_accepts_configured_driver(monkeypatch):
    """Con el driver configurado instalado no hay problemas."""
    _fake_pyodbc(monkeypatch, ['ODBC Driver 17 for SQL Server'])
    settings = Settings(synapse_workspace='ws', synapse_odbc_driver='ODBC Driver 17 for SQL Server')
    assert registry._synapse_check(settings) == []
