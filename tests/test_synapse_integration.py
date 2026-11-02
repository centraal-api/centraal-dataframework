"""Integración contra un Synapse real (pytest -m synapse -v).

Lee la configuración de las variables de entorno y, si existe, de `.env.synapse` en la raíz del repo
(ver docs/synapse.env.example) sin modificar os.environ. Se salta si no hay workspace/servidor o
si falta pyodbc.
"""

import os
from pathlib import Path

import pytest
from dotenv import dotenv_values

from centraal_dataframework.settings import Settings
from centraal_dataframework.synapse import SynapseClient, render

ENV_FILE = Path(__file__).resolve().parents[1] / '.env.synapse'
_FILE_VALUES = {k: v for k, v in dotenv_values(ENV_FILE).items() if v} if ENV_FILE.exists() else {}


def _var(name: str) -> str:
    """Valor de la variable desde el entorno o desde .env.synapse."""
    return os.environ.get(name) or _FILE_VALUES.get(name) or ''


pytestmark = [
    pytest.mark.synapse,
    pytest.mark.skipif(
        not (_var('SYNAPSE_WORKSPACE') or _var('SYNAPSE_SERVER')),
        reason='Synapse no configurado (SYNAPSE_WORKSPACE/SYNAPSE_SERVER o .env.synapse)',
    ),
]


@pytest.fixture
def client() -> SynapseClient:
    """Cliente real desde el entorno y .env.synapse."""
    pytest.importorskip('pyodbc')
    env_file = ENV_FILE if ENV_FILE.exists() else None
    return SynapseClient.from_settings(Settings(_env_file=env_file))


def test_select_1(client):
    """Conecta con la identidad configurada y ejecuta SELECT 1."""
    assert client.scalar('SELECT 1') == 1


def test_parameters_supported_on_serverless(client):
    """[supuesto] serverless acepta parámetros ? en un SELECT."""
    assert client.query('SELECT ? AS x', 'a') == [{'x': 'a'}]


def test_read_test_table(client):
    """Lee la tabla o vista de prueba si SYNAPSE_TEST_TABLE está definida."""
    table = _var('SYNAPSE_TEST_TABLE')
    if not table:
        pytest.skip('SYNAPSE_TEST_TABLE no definida')
    df = client.read_df(render('SELECT TOP 5 * FROM {{tabla}}', tabla=table))
    assert len(df.columns) > 0


def test_for_database(client):
    """Cambia de base con for_database si SYNAPSE_TEST_OTHER_DB está definida."""
    other = _var('SYNAPSE_TEST_OTHER_DB')
    if not other:
        pytest.skip('SYNAPSE_TEST_OTHER_DB no definida')
    assert client.for_database(other).scalar('SELECT DB_NAME()') == other


def _toolkit():
    """Toolkit GX efímero sin stores Azure."""
    pytest.importorskip('great_expectations')
    pytest.importorskip('sqlalchemy')
    from centraal_dataframework.resources import GreatExpectationsToolKit, get_context

    settings = Settings(azure_storage_connection_string=None, storage_connection_setting='NO_EXISTE')
    return GreatExpectationsToolKit(get_context(settings), 'integracion', settings)


def _sql_generado(caplog) -> str:
    """SQL que GX mandó al motor durante el bloque capturado."""
    return '\n'.join(r.getMessage() for r in caplog.records if r.name.startswith('sqlalchemy.engine'))


@pytest.fixture
def particion():
    """Vista de prueba con la trampa (dos fotos sanas y una carpeta con un Parquet dañado), columna y foto sana."""
    tabla = _var('SYNAPSE_TEST_PARTITION_TABLE')
    columna = _var('SYNAPSE_TEST_PARTITION_COLUMN')
    valor = _var('SYNAPSE_TEST_PARTITION_VALUE')
    if not (tabla and columna and valor):
        pytest.skip('SYNAPSE_TEST_PARTITION_TABLE / _COLUMN / _VALUE no definidas')
    return tabla, columna, valor


def test_gx_query_sql_with_columns_is_rejected(client):
    """validate_query en sql no admite expectativas de columna; en pandas la misma suite sí corre.

    GX no resuelve las columnas de una consulta suelta en SQL Server; el error remite a validate_table.
    """
    query, column = _var('SYNAPSE_TEST_GX_QUERY'), _var('SYNAPSE_TEST_GX_COLUMN')
    if not (query and column):
        pytest.skip('SYNAPSE_TEST_GX_QUERY / SYNAPSE_TEST_GX_COLUMN no definidas')
    gx = pytest.importorskip('great_expectations')
    toolkit = _toolkit()
    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=0),
        gx.expectations.ExpectColumnToExist(column=column),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=column, mostly=0.0),
    ]
    with pytest.raises(ValueError, match='validate_table'):
        toolkit.validate_query(client, 'gx_sql', query, expectations, modo='sql')
    pandas = toolkit.validate_query(client, 'gx_pandas', query, expectations, modo='pandas')
    assert pandas.success, pandas.failed


def test_validate_table_particion_sql_matches_pandas(client, caplog, particion):
    """La foto sana pasa en sql con expectativas de columna, la forzada falla, y pandas da lo mismo.

    El SQL generado lleva el filtro ``WHERE <col> = ?`` sin funciones, sin DISTINCT previo, sin la sonda
    ``TOP 1 1`` y sin tablas temporales: por eso no toca la carpeta con el Parquet dañado.
    """
    import logging

    gx = pytest.importorskip('great_expectations')
    tabla, columna, valor = particion
    toolkit = _toolkit()
    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
        gx.expectations.ExpectColumnToExist(column=columna),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columna),
        gx.expectations.ExpectColumnValuesToBeNull(column=columna),  # forzada: la partición nunca es nula
    ]
    with caplog.at_level(logging.INFO, logger='sqlalchemy.engine'):
        sql = toolkit.validate_table(
            client, 'tabla_sql', tabla, expectations, particion={columna: valor}, modo='sql', medir_costo=True
        )
    generado = _sql_generado(caplog)
    pandas = toolkit.validate_table(
        client, 'tabla_pandas', tabla, expectations, particion={columna: valor}, modo='pandas', medir_costo=True
    )
    print('\n--- SQL generado por GX ---\n' + generado)
    print(f"MB procesados: sql={sql.data_processed_mb} pandas={pandas.data_processed_mb}")

    assert sql.failed == ['expect_column_values_to_be_null'], sql.failed
    assert pandas.failed == sql.failed
    texto = generado.lower()
    assert f'where {columna.lower()} = ?' in texto
    assert 'distinct' not in texto
    assert 'top 1 1' not in texto
    assert ' #' not in texto and 'into #' not in texto
    assert not any(f'{f}({columna.lower()})' in texto for f in ('year', 'month', 'day', 'datepart'))


def test_validate_table_completa_falla_por_trampa(client, particion):
    """particion='completa' recorre la carpeta dañada y termina en ErrorMotorCalidad, no en fallo de calidad.

    Demuestra que la trampa funciona: la validación por partición no la tocó.
    """
    gx = pytest.importorskip('great_expectations')
    from centraal_dataframework import ErrorMotorCalidad

    tabla, _columna, _valor = particion
    with pytest.raises(ErrorMotorCalidad) as info:
        _toolkit().validate_table(
            client,
            'completa',
            tabla,
            [gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)],
            particion='completa',
            modo='sql',
        )
    assert 'parquet' in str(info.value).lower()


def test_validate_table_sin_particion_se_niega(client, particion):
    """Una vista particionada sin particion declarada no se valida entera por descuido."""
    gx = pytest.importorskip('great_expectations')
    from centraal_dataframework import ParticionInvalida

    tabla, _columna, _valor = particion
    with pytest.raises(ParticionInvalida, match='completa'):
        _toolkit().validate_table(client, 'sin', tabla, [gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)])


def test_validate_table_columna_que_no_es_particion(client, particion):
    """Declarar como partición una columna de datos se rechaza contra la definición de la vista."""
    gx = pytest.importorskip('great_expectations')
    from centraal_dataframework import ParticionInvalida

    tabla, columna, _valor = particion
    otra = next(c for c in client.read_df(f'SELECT TOP 0 * FROM {tabla}').columns if c.lower() != columna.lower())
    with pytest.raises(ParticionInvalida, match='filepath'):
        _toolkit().validate_table(
            client, 'otra', tabla, [gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)], particion={otra: 1}
        )
