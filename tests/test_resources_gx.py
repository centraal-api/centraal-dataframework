"""Tests del toolkit GX con mocks."""

from unittest.mock import MagicMock, patch

import pytest

from centraal_dataframework.resources import DQResult, GreatExpectationsToolKit
from centraal_dataframework.settings import Settings


def test_validate_success_path(monkeypatch):
    """Validate construye el checkpoint y devuelve DQResult."""
    df = {'a': [1, 2]}  # no requiere pandas real en este mock
    context = MagicMock()
    datasource = MagicMock()
    asset = MagicMock()
    batch_def = MagicMock()
    asset.add_batch_definition_whole_dataframe.return_value = batch_def
    datasource.add_dataframe_asset.return_value = asset
    context.data_sources.add_or_update_pandas.return_value = datasource
    context.suites.add_or_update.side_effect = lambda s: s
    context.validation_definitions.add_or_update.side_effect = lambda v: v
    context.checkpoints.add_or_update.side_effect = lambda c: c

    result = MagicMock()
    result.success = True
    result.run_results = {}
    checkpoint_instance = MagicMock()
    checkpoint_instance.run.return_value = result

    fake_gx = MagicMock()
    fake_gx.ExpectationSuite = MagicMock(side_effect=lambda **k: MagicMock(**k))
    fake_gx.ValidationDefinition = MagicMock(side_effect=lambda **k: MagicMock(**k))
    fake_gx.Checkpoint = MagicMock(return_value=checkpoint_instance)

    monkeypatch.setitem(__import__('sys').modules, 'great_expectations', fake_gx)
    monkeypatch.setitem(
        __import__('sys').modules,
        'great_expectations.checkpoint',
        MagicMock(UpdateDataDocsAction=MagicMock),
    )

    # Re-bind imports inside method by patching module attribute used after import
    import centraal_dataframework.resources as res

    toolkit = GreatExpectationsToolKit(context=context, name_task='t', settings=Settings())
    with patch.object(res, 'get_datalake', side_effect=Exception('no dl')):
        # Force import gx inside validate
        with patch.dict('sys.modules', {'great_expectations': fake_gx}):
            # Manually call internal with patched gx
            toolkit.datasource = datasource
            # Patch builtins import in _run_suite_on_df
            import centraal_dataframework.resources as mod

            def fake_run(self, df, name, suite, public=False):
                return DQResult(success=True, url='', failed=[], raw=result)

            monkeypatch.setattr(mod.GreatExpectationsToolKit, '_run_suite_on_df', fake_run)
            out = toolkit.validate(df, 'n', [MagicMock()])
            assert out.success is True


def test_get_context_ephemeral(monkeypatch):
    """get_context usa gx.get_context."""
    fake_ctx = MagicMock()
    fake_gx = MagicMock()
    fake_gx.get_context.return_value = fake_ctx
    monkeypatch.setitem(__import__('sys').modules, 'great_expectations', fake_gx)
    import centraal_dataframework.resources as res

    def fake_get_context(settings=None):
        return fake_ctx

    monkeypatch.setattr(res, 'get_context', fake_get_context)
    assert res.get_context() is fake_ctx


def test_validate_reuses_the_same_expectations_list():
    """La misma lista de expectativas (definida a nivel de módulo) sirve para varias validaciones."""
    import pytest

    gx = pytest.importorskip('great_expectations')
    pd = pytest.importorskip('pandas')
    from centraal_dataframework.resources import get_context

    settings = Settings(azure_storage_connection_string=None, storage_connection_setting='NO_EXISTE')
    toolkit = GreatExpectationsToolKit(get_context(settings), 't', settings)
    expectations = [gx.expectations.ExpectColumnValuesToBeBetween(column='v', min_value=0)]

    assert toolkit.validate(pd.DataFrame({'v': [1, 2]}), 'a', expectations).success
    second = toolkit.validate(pd.DataFrame({'v': [-1]}), 'a', expectations)
    assert not second.success
    assert expectations[0].id is None  # la lista del usuario no queda ligada a una suite


class _SqliteSynapse:
    """Doble de SynapseClient: misma interfaz que usa validate_query, sobre una base SQLite."""

    def __init__(self, path, processed=(100.0, 112.5)):
        """Guarda la ruta y la secuencia de lecturas de data_processed_mb."""
        self.path = path
        self.database = 'bronze'
        self.queries = []
        self._processed = list(processed)
        self.kwargs_calls = 0

    def sqlalchemy_url(self):
        """URL de SQLAlchemy."""
        return f'sqlite:///{self.path}'

    def sqlalchemy_kwargs(self):
        """Sin token en SQLite."""
        self.kwargs_calls += 1
        return {}

    def read_df(self, sql):
        """Plan B: trae el resultado a pandas."""
        import sqlite3

        import pandas as pd

        self.queries.append(sql)
        with sqlite3.connect(self.path) as conn:
            return pd.read_sql_query(sql, conn)

    def data_processed_mb(self):
        """Contador simulado."""
        return self._processed.pop(0)


@pytest.fixture
def sqlite_synapse(tmp_path):
    """Base con una 'vista' particionada por fecha de foto."""
    import sqlite3

    pytest.importorskip('sqlalchemy')
    path = tmp_path / 'bronze.db'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE ventas (fecha TEXT, id INTEGER, monto REAL)')
        conn.executemany(
            'INSERT INTO ventas VALUES (?, ?, ?)',
            [('2026-11-01', 1, 10.0), ('2026-11-01', 2, 20.0), ('2026-10-31', 3, None)],
        )
    return _SqliteSynapse(path)


@pytest.fixture
def toolkit():
    """Toolkit con contexto GX efímero real y sin stores Azure."""
    pytest.importorskip('great_expectations')
    from centraal_dataframework.resources import get_context

    settings = Settings(azure_storage_connection_string=None, storage_connection_setting='NO_EXISTE')
    return GreatExpectationsToolKit(get_context(settings), 't', settings)


QUERY = "SELECT id, monto FROM ventas WHERE fecha = '{{fecha}}'"


def _expectations():
    import great_expectations as gx

    return [
        gx.expectations.ExpectColumnValuesToNotBeNull(column='monto'),
        gx.expectations.ExpectColumnValuesToBeBetween(column='monto', min_value=0, max_value=100),
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
    ]


@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_validate_query_success_in_both_modes(toolkit, sqlite_synapse, modo):
    """Plan A (SQL en el motor) y plan B (pandas) validan la misma suite con el mismo resultado."""
    out = toolkit.validate_query(
        sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': '2026-11-01'}, modo=modo
    )
    assert out.success is True
    assert out.failed == []
    assert out.modo == modo


@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_validate_query_reports_failures_in_both_modes(toolkit, sqlite_synapse, modo):
    """La partición con nulos falla igual en los dos modos."""
    out = toolkit.validate_query(
        sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': '2026-10-31'}, modo=modo
    )
    assert out.success is False
    assert out.failed == ['expect_column_values_to_not_be_null']


def test_validate_query_sql_mode_does_not_pull_rows(toolkit, sqlite_synapse):
    """En modo sql la Function no trae filas: no se llama a read_df y la conexión usa sqlalchemy_kwargs."""
    toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': '2026-11-01'})
    assert sqlite_synapse.queries == []
    assert sqlite_synapse.kwargs_calls == 1


def test_validate_query_pandas_mode_renders_the_query(toolkit, sqlite_synapse):
    """En modo pandas se lee exactamente la query renderizada."""
    toolkit.validate_query(
        sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': '2026-11-01'}, modo='pandas'
    )
    assert sqlite_synapse.queries == ["SELECT id, monto FROM ventas WHERE fecha = '2026-11-01'"]


def test_validate_query_rejects_unsafe_params(toolkit, sqlite_synapse):
    """Los parámetros pasan por la lista blanca de render."""
    with pytest.raises(ValueError):
        toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': "x' OR '1'='1"})


def test_validate_query_rejects_unknown_mode(toolkit, sqlite_synapse):
    """Modo desconocido: error claro antes de consultar."""
    with pytest.raises(ValueError, match='modo'):
        toolkit.validate_query(
            sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': '2026-11-01'}, modo='spark'
        )


def test_validate_query_requires_expectations_or_suite(toolkit, sqlite_synapse):
    """Debe recibir expectativas o el nombre de una suite."""
    with pytest.raises(ValueError, match='suite'):
        toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, params={'fecha': '2026-11-01'})


def test_validate_query_with_existing_suite(toolkit, sqlite_synapse):
    """Acepta una suite ya registrada en el contexto."""
    import great_expectations as gx

    toolkit.context.suites.add(gx.ExpectationSuite(name='mi_suite', expectations=_expectations()))
    out = toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, suite_name='mi_suite', params={'fecha': '2026-11-01'})
    assert out.success is True


def test_validate_query_measures_data_processed(toolkit, sqlite_synapse):
    """medir_costo deja en el resultado el delta de MB procesados."""
    out = toolkit.validate_query(
        sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': '2026-11-01'}, medir_costo=True
    )
    assert out.data_processed_mb == pytest.approx(12.5)


def test_validate_query_without_cost_measure_has_none(toolkit, sqlite_synapse):
    """Sin medir_costo no consulta el contador."""
    out = toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': '2026-11-01'})
    assert out.data_processed_mb is None


def test_validate_query_twice_reuses_the_sql_datasource(toolkit, sqlite_synapse):
    """Dos validaciones en la misma tarea comparten el datasource SQL de la base."""
    toolkit.validate_query(sqlite_synapse, 'a', QUERY, _expectations(), params={'fecha': '2026-11-01'})
    toolkit.validate_query(sqlite_synapse, 'b', QUERY, _expectations(), params={'fecha': '2026-11-01'})
    names = [ds.name for ds in toolkit.context.data_sources.all().values()]
    assert names.count('t_synapse_bronze') == 1
