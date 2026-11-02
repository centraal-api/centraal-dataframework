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

    def __init__(self, path, processed=(100.0, 112.5), definicion=None, tipo='V'):
        """Guarda la ruta, la secuencia de lecturas de data_processed_mb y la definición/tipo del objeto."""
        self.path = path
        self.database = 'bronze'
        self.queries = []
        self._processed = list(processed)
        self.kwargs_calls = 0
        self.definicion = definicion
        self.tipo = tipo

    def tipo_objeto(self, tabla):
        """Tipo simulado del objeto ('V', 'U' o None)."""
        return self.tipo

    def definicion_objeto(self, tabla):
        """Definición simulada de la vista (None = ilegible)."""
        return self.definicion

    def sqlalchemy_url(self):
        """URL de SQLAlchemy."""
        return f'sqlite:///{self.path}'

    def sqlalchemy_kwargs(self):
        """Sin token en SQLite."""
        self.kwargs_calls += 1
        return {}

    def read_df(self, sql, *params):
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


def _expectations_tabla(min_value=1):
    """Solo expectativas de tabla: las únicas que validate_query admite en modo sql."""
    import great_expectations as gx

    return [gx.expectations.ExpectTableRowCountToBeBetween(min_value=min_value)]


def _exps(modo):
    """La suite por modo: en sql solo de tabla, en pandas también de columna."""
    return _expectations_tabla() if modo == 'sql' else _expectations()


@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_validate_query_success_in_both_modes(toolkit, sqlite_synapse, modo):
    """Plan A (SQL en el motor, expectativas de tabla) y plan B (pandas) validan con éxito."""
    out = toolkit.validate_query(
        sqlite_synapse, 'ventas', QUERY, _exps(modo), params={'fecha': '2026-11-01'}, modo=modo
    )
    assert out.success is True
    assert out.failed == []
    assert out.modo == modo


@pytest.mark.parametrize(
    'modo, exps, fallida',
    [
        ('sql', lambda: _expectations_tabla(min_value=2), 'expect_table_row_count_to_be_between'),
        ('pandas', _expectations, 'expect_column_values_to_not_be_null'),
    ],
)
def test_validate_query_reports_failures_in_both_modes(toolkit, sqlite_synapse, modo, exps, fallida):
    """La partición de una fila (y con nulos) falla en los dos modos, cada uno con su expectativa."""
    out = toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, exps(), params={'fecha': '2026-10-31'}, modo=modo)
    assert out.success is False
    assert out.failed == [fallida]


def test_validate_query_sql_rejects_column_expectations(toolkit, sqlite_synapse):
    """En modo sql una expectativa de columna da error claro antes de conectarse: use validate_table o pandas."""
    with pytest.raises(ValueError, match='validate_table'):
        toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, _expectations(), params={'fecha': '2026-11-01'})
    assert sqlite_synapse.kwargs_calls == 0
    assert sqlite_synapse.queries == []


def test_validate_query_sql_allows_table_expectations(toolkit, sqlite_synapse):
    """En modo sql las expectativas de tabla siguen corriendo en el motor."""
    out = toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, _expectations_tabla(), params={'fecha': '2026-11-01'})
    assert out.success is True
    assert sqlite_synapse.queries == []


def test_validate_query_sql_mode_does_not_pull_rows(toolkit, sqlite_synapse):
    """En modo sql la Function no trae filas: no se llama a read_df y la conexión usa sqlalchemy_kwargs."""
    toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, _expectations_tabla(), params={'fecha': '2026-11-01'})
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

    toolkit.context.suites.add(gx.ExpectationSuite(name='mi_suite', expectations=_expectations_tabla()))
    out = toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, suite_name='mi_suite', params={'fecha': '2026-11-01'})
    assert out.success is True


def test_validate_query_measures_data_processed(toolkit, sqlite_synapse):
    """medir_costo deja en el resultado el delta de MB procesados."""
    out = toolkit.validate_query(
        sqlite_synapse, 'ventas', QUERY, _expectations_tabla(), params={'fecha': '2026-11-01'}, medir_costo=True
    )
    assert out.data_processed_mb == pytest.approx(12.5)


def test_validate_query_without_cost_measure_has_none(toolkit, sqlite_synapse):
    """Sin medir_costo no consulta el contador."""
    out = toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, _expectations_tabla(), params={'fecha': '2026-11-01'})
    assert out.data_processed_mb is None


def test_validate_query_twice_reuses_the_sql_datasource(toolkit, sqlite_synapse):
    """Dos validaciones en la misma tarea comparten el datasource SQL de la base."""
    toolkit.validate_query(sqlite_synapse, 'a', QUERY, _expectations_tabla(), params={'fecha': '2026-11-01'})
    toolkit.validate_query(sqlite_synapse, 'b', QUERY, _expectations_tabla(), params={'fecha': '2026-11-01'})
    names = [ds.name for ds in toolkit.context.data_sources.all().values()]
    assert names.count('t_synapse_bronze') == 1


@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_run_checkpoint_raises_on_metric_exception(toolkit, sqlite_synapse, modo):
    """Un error técnico de GX (columna inexistente) es ErrorMotorCalidad, no un fallo de calidad."""
    import great_expectations as gx

    from centraal_dataframework import ErrorMotorCalidad

    if modo == 'sql':
        pytest.skip('en sql validate_query rechaza columnas; ver test_validate_table_sql_raises_on_metric_exception')
    exps = [gx.expectations.ExpectColumnValuesToNotBeNull(column='no_existe')]
    with pytest.raises(ErrorMotorCalidad) as info:
        toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, exps, params={'fecha': '2026-11-01'}, modo=modo)
    assert info.value.modo == modo and info.value.mensajes


# --- validate_table -------------------------------------------------------------------------------------------

from tests.test_particiones import DEF_SIMPLE, DEF_SIN_FILEPATH  # noqa: E402


@pytest.fixture
def vista_particionada(sqlite_synapse):
    """La tabla ventas de SQLite presentada como vista de bronze particionada por fecha=*."""
    sqlite_synapse.definicion = DEF_SIMPLE
    return sqlite_synapse


def _exps_id():
    import great_expectations as gx

    return [
        gx.expectations.ExpectColumnValuesToNotBeNull(column='monto'),
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
    ]


@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_validate_table_particion_ok(toolkit, vista_particionada, modo):
    """La foto sana pasa en los dos modos con expectativas de columna."""
    out = toolkit.validate_table(
        vista_particionada, 'ventas', 'main.ventas', _exps_id(), particion={'fecha': '2026-11-01'}, modo=modo
    )
    assert out.success is True and out.failed == [] and out.modo == modo


@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_validate_table_particion_reports_failures(toolkit, vista_particionada, modo):
    """La foto con nulos falla igual en los dos modos."""
    out = toolkit.validate_table(
        vista_particionada, 'ventas', 'main.ventas', _exps_id(), particion={'fecha': '2026-10-31'}, modo=modo
    )
    assert out.success is False and out.failed == ['expect_column_values_to_not_be_null']


def test_validate_table_sql_does_not_pull_rows(toolkit, vista_particionada):
    """En sql no se llama a read_df y la conexión usa sqlalchemy_kwargs."""
    toolkit.validate_table(vista_particionada, 'v', 'main.ventas', _exps_id(), particion={'fecha': '2026-11-01'})
    assert vista_particionada.queries == [] and vista_particionada.kwargs_calls == 1


def test_validate_table_pandas_reads_filtered_select(toolkit, vista_particionada):
    """En pandas se trae la tabla filtrada por la partición con el valor validado como literal."""
    toolkit.validate_table(
        vista_particionada, 'v', 'main.ventas', _exps_id(), particion={'fecha': '2026-11-01'}, modo='pandas'
    )
    assert vista_particionada.queries == ["SELECT * FROM main.ventas WHERE fecha = '2026-11-01'"]


def test_validate_table_sin_particion_en_vista_particionada(toolkit, vista_particionada):
    """Una vista particionada sin particion declarada se rechaza: hay que declararla o pedir 'completa'."""
    from centraal_dataframework import ParticionInvalida

    with pytest.raises(ParticionInvalida, match='completa'):
        toolkit.validate_table(vista_particionada, 'v', 'main.ventas', _exps_id())
    assert vista_particionada.kwargs_calls == 0


def test_validate_table_completa(toolkit, vista_particionada):
    """particion='completa' valida toda la vista a propósito (aquí falla por el nulo del total)."""
    out = toolkit.validate_table(vista_particionada, 'v', 'main.ventas', _exps_id(), particion='completa')
    assert out.success is False and out.failed == ['expect_column_values_to_not_be_null']


def test_validate_table_columna_no_particion(toolkit, vista_particionada):
    """Una columna de datos no es partición."""
    from centraal_dataframework import ParticionInvalida

    with pytest.raises(ParticionInvalida, match='no sale de filepath'):
        toolkit.validate_table(vista_particionada, 'v', 'main.ventas', _exps_id(), particion={'id': 1})


def test_validate_table_definicion_ilegible(toolkit, sqlite_synapse):
    """Vista sin definición legible: falta GRANT VIEW DEFINITION, con o sin particion."""
    from centraal_dataframework import ParticionInvalida

    sqlite_synapse.definicion = None
    with pytest.raises(ParticionInvalida, match='VIEW DEFINITION'):
        toolkit.validate_table(sqlite_synapse, 'v', 'main.ventas', _exps_id(), particion={'fecha': '2026-11-01'})
    with pytest.raises(ParticionInvalida, match='VIEW DEFINITION'):
        toolkit.validate_table(sqlite_synapse, 'v', 'main.ventas', _exps_id())


def test_validate_table_objeto_inexistente(toolkit, sqlite_synapse):
    """Objeto que no existe o que la identidad no ve."""
    from centraal_dataframework import ParticionInvalida

    sqlite_synapse.tipo = None
    with pytest.raises(ParticionInvalida, match='no existe'):
        toolkit.validate_table(sqlite_synapse, 'v', 'main.ventas', _exps_id())


def test_validate_table_tabla_externa_entera(toolkit, sqlite_synapse):
    """Una tabla (no vista) se valida entera sin leer definición."""
    sqlite_synapse.tipo = 'U'
    out = toolkit.validate_table(sqlite_synapse, 'v', 'main.ventas', _exps_id(), modo='pandas')
    assert out.success is False and sqlite_synapse.queries == ['SELECT * FROM main.ventas']


def test_validate_table_tabla_externa_con_particion(toolkit, sqlite_synapse):
    """Una tabla (no vista) no tiene particiones de carpeta que aplicar."""
    from centraal_dataframework import ParticionInvalida

    sqlite_synapse.tipo = 'U'
    with pytest.raises(ParticionInvalida, match='no es una vista'):
        toolkit.validate_table(sqlite_synapse, 'v', 'main.ventas', _exps_id(), particion={'fecha': '2026-11-01'})


def test_validate_table_vista_sin_filepath_entera(toolkit, sqlite_synapse):
    """Una vista sin filepath (silver/gold) se valida entera sin declarar particion."""
    sqlite_synapse.definicion = DEF_SIN_FILEPATH
    out = toolkit.validate_table(sqlite_synapse, 'v', 'main.ventas', _exps_id(), modo='pandas')
    assert out.success is False and sqlite_synapse.queries == ['SELECT * FROM main.ventas']


def test_validate_table_rejects_unsafe_partition_value(toolkit, vista_particionada):
    """El valor de la partición pasa por la lista blanca antes de tocar Synapse."""
    with pytest.raises(ValueError):
        toolkit.validate_table(
            vista_particionada, 'v', 'main.ventas', _exps_id(), particion={'fecha': "2026-11-01' OR 1=1"}
        )
    assert vista_particionada.kwargs_calls == 0


@pytest.mark.parametrize('particion', [{'a': 1, 'b': 2}, {}, 'otra', 5, {'no-valido': 1}])
def test_validate_table_particion_shape(toolkit, vista_particionada, particion):
    """La partición es None, 'completa' o un dict de una sola entrada con nombre de columna válido."""
    with pytest.raises(ValueError, match='particion'):
        toolkit.validate_table(vista_particionada, 'v', 'main.ventas', _exps_id(), particion=particion)


def test_validate_table_base_distinta(toolkit, vista_particionada):
    """base.esquema.tabla con otra base se rechaza antes de conectarse."""
    with pytest.raises(ValueError, match='no coincide'):
        toolkit.validate_table(vista_particionada, 'v', 'silver.main.ventas', _exps_id(), particion='completa')


def test_validate_table_measures_data_processed(toolkit, vista_particionada):
    """medir_costo deja el delta de MB procesados."""
    out = toolkit.validate_table(
        vista_particionada, 'v', 'main.ventas', _exps_id(), particion={'fecha': '2026-11-01'}, medir_costo=True
    )
    assert out.data_processed_mb == pytest.approx(12.5)


def test_validate_table_sql_raises_on_metric_exception(toolkit, vista_particionada):
    """Un error técnico de GX en sql (columna inexistente) es ErrorMotorCalidad."""
    import great_expectations as gx

    from centraal_dataframework import ErrorMotorCalidad

    exps = [gx.expectations.ExpectColumnValuesToNotBeNull(column='no_existe')]
    with pytest.raises(ErrorMotorCalidad) as info:
        toolkit.validate_table(vista_particionada, 'v', 'main.ventas', exps, particion={'fecha': '2026-11-01'})
    assert info.value.modo == 'sql'


@pytest.mark.parametrize('valor', [None, True, False])
def test_validate_table_rejects_none_and_bool_partition_values(toolkit, vista_particionada, valor):
    """None (parámetro ausente) y bool no son valores de partición: ValueError antes de tocar Synapse."""
    with pytest.raises(ValueError, match='particion'):
        toolkit.validate_table(vista_particionada, 'v', 'main.ventas', _exps_id(), particion={'fecha': valor})
    assert vista_particionada.kwargs_calls == 0


def test_validate_table_accepts_date_partition_value(toolkit, vista_particionada):
    """Un date se normaliza a ISO y filtra igual que la cadena."""
    import datetime as dt

    toolkit.validate_table(
        vista_particionada, 'v', 'main.ventas', _exps_id(), particion={'fecha': dt.date(2026, 11, 1)}, modo='pandas'
    )
    assert vista_particionada.queries == ["SELECT * FROM main.ventas WHERE fecha = '2026-11-01'"]


def test_validate_dataframe_missing_column_is_a_quality_failure(toolkit):
    """Fuera de Synapse, validate(df) conserva su semántica: una columna ausente es fallo de calidad, no error de motor."""
    import great_expectations as gx
    import pandas as pd

    df = pd.DataFrame({'id': [1, 2]})
    out = toolkit.validate(df, 'df', [gx.expectations.ExpectColumnValuesToNotBeNull(column='no_existe')])
    assert out.success is False
    assert out.failed == ['expect_column_values_to_not_be_null']
