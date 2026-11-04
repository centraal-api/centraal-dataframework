"""Tests de historial (sin Azure real)."""

import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

from centraal_dataframework.history import RunHistory
from centraal_dataframework.settings import Settings


def test_history_disabled():
    """Sin HISTORY_ENABLED no hace nada."""
    h = RunHistory(Settings(history_enabled=False))
    assert h.enabled is False
    h.record_start('t', 'r', {})
    assert h.get_run('r') is None


def test_history_record_with_mock_table(monkeypatch):
    """Upsert/list con table mock."""
    settings = Settings(history_enabled=True, azure_storage_connection_string='x=y')
    table = MagicMock()
    service = MagicMock()
    service.create_table_if_not_exists.return_value = table

    class FakeTSC:
        @staticmethod
        def from_connection_string(conn):
            return service

    monkeypatch.setitem(
        __import__('sys').modules,
        'azure.data.tables',
        MagicMock(TableServiceClient=FakeTSC, UpdateMode=MagicMock(MERGE='merge')),
    )
    # Re-import path used inside RunHistory
    import centraal_dataframework.history as hist_mod

    monkeypatch.setattr(
        hist_mod,
        'RunHistory',
        RunHistory,
    )

    h = RunHistory.__new__(RunHistory)
    h.settings = settings
    h._table = table

    started = datetime.now(timezone.utc)
    h.record_start('alpha', 'rid', {'a': 1})
    h.record_success('alpha', 'rid', started)
    h.record_failure('alpha', 'rid2', started, RuntimeError('e'), url='http://x')
    table.upsert_entity.assert_called()
    table.list_entities.return_value = [{'RowKey': 'rid', 'status': 'success'}]
    assert h.get_run('rid')['status'] == 'success'
    table.query_entities.return_value = [{'RowKey': 'rid'}]
    assert h.list_runs(task_name='alpha')


def _modulos_falsos(monkeypatch, registro):
    """Módulos azure.data.tables y azure.identity falsos: registran el endpoint y la credencial usados."""

    class FakeTSC:
        def __init__(self, endpoint, credential=None):
            registro['endpoint'] = endpoint
            registro['credential'] = credential

        @staticmethod
        def from_connection_string(conn):
            registro['conn'] = conn
            return FakeTSC('conn', None)

        def create_table_if_not_exists(self, name):
            registro['tabla'] = name
            return MagicMock()

    class FakeCredential:
        pass

    modules = __import__('sys').modules
    monkeypatch.setitem(modules, 'azure.data.tables', MagicMock(TableServiceClient=FakeTSC, UpdateMode=MagicMock()))
    monkeypatch.setitem(modules, 'azure.identity', MagicMock(DefaultAzureCredential=FakeCredential))
    return FakeCredential


def test_history_con_identidad_por_account_name(monkeypatch):
    """Sin connection string, con <conexión>__accountName, el historial usa la identidad administrada."""
    monkeypatch.delenv('AzureWebJobsStorage', raising=False)
    monkeypatch.setenv('AzureWebJobsStorage__accountName', 'stcuenta')
    registro = {}
    credencial = _modulos_falsos(monkeypatch, registro)
    settings = Settings(_env_file=None, history_enabled=True, azure_storage_connection_string=None)

    h = RunHistory(settings)

    assert h.enabled is True
    assert registro['endpoint'] == 'https://stcuenta.table.core.windows.net'
    assert isinstance(registro['credential'], credencial)
    assert registro['tabla'] == 'taskruns'


def test_history_con_identidad_por_storage_account_name(monkeypatch):
    """STORAGE_ACCOUNT_NAME también sirve para el historial por identidad."""
    monkeypatch.delenv('AzureWebJobsStorage', raising=False)
    monkeypatch.delenv('AzureWebJobsStorage__accountName', raising=False)
    registro = {}
    _modulos_falsos(monkeypatch, registro)
    settings = Settings(
        _env_file=None, history_enabled=True, azure_storage_connection_string=None, storage_account_name='stotra'
    )

    assert RunHistory(settings).enabled is True
    assert registro['endpoint'] == 'https://stotra.table.core.windows.net'


def test_history_sin_conexion_ni_cuenta_avisa(monkeypatch, caplog):
    """Sin connection string ni cuenta, el historial queda desactivado y el aviso nombra las dos opciones."""
    monkeypatch.delenv('AzureWebJobsStorage', raising=False)
    monkeypatch.delenv('AzureWebJobsStorage__accountName', raising=False)
    settings = Settings(
        _env_file=None, history_enabled=True, azure_storage_connection_string=None, storage_account_name=None
    )
    with caplog.at_level('WARNING'):
        h = RunHistory(settings)
    assert h.enabled is False
    assert 'AzureWebJobsStorage__accountName' in caplog.text


def _tabla_con_merge():
    """Tabla falsa con la semántica de MERGE de Table Storage: lo que no se envía se conserva."""
    filas = {}

    class Tabla:
        def upsert_entity(self, entity, mode=None):
            clave = (entity['PartitionKey'], entity['RowKey'])
            filas.setdefault(clave, {}).update(entity)

        def get_entity(self, partition_key, row_key):
            return filas[(partition_key, row_key)]

    return Tabla(), filas


def _historial_con_merge(monkeypatch):
    monkeypatch.setitem(sys.modules, 'azure.data.tables', MagicMock(UpdateMode=MagicMock(MERGE='merge')))
    tabla, filas = _tabla_con_merge()
    h = RunHistory.__new__(RunHistory)
    h.settings = Settings(history_enabled=True)
    h._table = tabla
    return h, filas


def test_record_success_limpia_el_error_de_intentos_anteriores(monkeypatch):
    """Un run_id que falló y luego pasó queda 'success' sin el texto del error viejo ni su url."""
    h, filas = _historial_con_merge(monkeypatch)
    started = datetime.now(timezone.utc)

    h.record_start('alpha', 'rid', {'a': 1})
    h.record_failure('alpha', 'rid', started, RuntimeError('se cayó'), url='http://viejo')
    h.record_success('alpha', 'rid', started)

    fila = filas[('alpha', 'rid')]
    assert fila['status'] == 'success'
    assert fila['error'] == ''
    assert fila['url'] == ''
    assert fila['params'] == "{'a': 1}"


def test_record_success_conserva_la_url_del_resultado(monkeypatch):
    """Si el resultado trae url (por ejemplo los data docs de una validación), esa es la que queda."""
    h, filas = _historial_con_merge(monkeypatch)
    started = datetime.now(timezone.utc)

    h.record_failure('alpha', 'rid', started, RuntimeError('se cayó'), url='http://viejo')
    h.record_success('alpha', 'rid', started, result=SimpleNamespace(url='http://nuevo'))

    assert filas[('alpha', 'rid')]['url'] == 'http://nuevo'
    assert filas[('alpha', 'rid')]['error'] == ''
