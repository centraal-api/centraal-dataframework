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
