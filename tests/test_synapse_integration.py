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


def test_gx_native_sql_matches_pandas(client, caplog):
    """Plan A: GX ejecuta la suite en serverless; da lo mismo que el plan B y se guarda el SQL generado.

    Usa SYNAPSE_TEST_GX_QUERY (un SELECT con el filtro de partición) y SYNAPSE_TEST_GX_COLUMN (una columna
    de esa consulta). Corre en la base de ``client`` (SYNAPSE_DATABASE).
    """
    import logging

    query, column = _var('SYNAPSE_TEST_GX_QUERY'), _var('SYNAPSE_TEST_GX_COLUMN')
    if not (query and column):
        pytest.skip('SYNAPSE_TEST_GX_QUERY / SYNAPSE_TEST_GX_COLUMN no definidas')
    gx = pytest.importorskip('great_expectations')
    pytest.importorskip('sqlalchemy')
    from centraal_dataframework.resources import GreatExpectationsToolKit, get_context

    settings = Settings(azure_storage_connection_string=None, storage_connection_setting='NO_EXISTE')
    toolkit = GreatExpectationsToolKit(get_context(settings), 'integracion', settings)
    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=0),
        gx.expectations.ExpectColumnToExist(column=column),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=column, mostly=0.0),
    ]

    with caplog.at_level(logging.INFO, logger='sqlalchemy.engine'):
        sql = toolkit.validate_query(client, 'gx_sql', query, expectations, modo='sql', medir_costo=True)
    generated = [r.getMessage() for r in caplog.records if r.name.startswith('sqlalchemy.engine')]
    pandas = toolkit.validate_query(client, 'gx_pandas', query, expectations, modo='pandas', medir_costo=True)

    print('\n--- SQL generado por GX ---\n' + '\n'.join(generated))
    print(f"MB procesados: sql={sql.data_processed_mb} pandas={pandas.data_processed_mb}")
    assert sql.success and pandas.success, (sql.failed, pandas.failed)
    assert generated, 'GX no ejecutó SQL en el motor'
