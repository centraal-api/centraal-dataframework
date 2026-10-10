"""Tests de historial (sin Azure real)."""

from datetime import datetime, timezone
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
