"""TableAssetSynapse: table asset de GX sin sonda sobre toda la tabla ni DISTINCT de la columna de partición."""

from __future__ import annotations

import logging

import pytest

from tests.test_resources_gx import _SqliteSynapse, sqlite_synapse, toolkit  # noqa: F401 fixtures


def _suite_conteo(toolkit, nombre):
    import great_expectations as gx

    return toolkit._build_suite(nombre, [gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)])


def _sql_emitido(caplog):
    return '\n'.join(r.getMessage() for r in caplog.records if r.name.startswith('sqlalchemy.engine')).lower()


def test_comprobar_api_gx_no_lanza():
    """Los nombres privados de GX de los que depende TableAssetSynapse existen en la versión instalada."""
    from centraal_dataframework.gx_synapse import comprobar_api_gx

    comprobar_api_gx()


def test_table_asset_sin_sonda_ni_distinct(toolkit, sqlite_synapse, caplog):  # noqa: F811
    """Con partición especificada no hay SELECT DISTINCT ni LIMIT 1 de prueba; sí WHERE fecha = ?."""
    with caplog.at_level(logging.INFO, logger='sqlalchemy.engine'):
        asset_name, bd, params = toolkit._table_batch_definition(
            sqlite_synapse, 'v', 'main', 'ventas', ('fecha', '2026-11-01')
        )
        out = toolkit._run_checkpoint(asset_name, bd, _suite_conteo(toolkit, 'v'), params, name='v', modo='sql')
    sql = _sql_emitido(caplog)
    assert out.success is True
    assert 'distinct' not in sql
    assert 'limit 1' not in sql and 'top 1' not in sql
    assert 'where fecha = ?' in sql
    assert sqlite_synapse.queries == []


def test_table_asset_whole_table(toolkit, sqlite_synapse, caplog):  # noqa: F811
    """Sin partición se valida la tabla entera, sin DISTINCT."""
    with caplog.at_level(logging.INFO, logger='sqlalchemy.engine'):
        asset_name, bd, params = toolkit._table_batch_definition(sqlite_synapse, 'w', 'main', 'ventas', None)
        out = toolkit._run_checkpoint(asset_name, bd, _suite_conteo(toolkit, 'w'), params, name='w', modo='sql')
    sql = _sql_emitido(caplog)
    assert out.success is True and params == {}
    assert 'distinct' not in sql
    assert 'where 1 = 1' in sql


def test_table_asset_particion_inexistente_no_valida_en_vacio(toolkit, sqlite_synapse):  # noqa: F811
    """Un valor sin filas da row_count 0: la expectativa de conteo falla, no pasa en vacío."""
    asset_name, bd, params = toolkit._table_batch_definition(
        sqlite_synapse, 'x', 'main', 'ventas', ('fecha', '1900-01-01')
    )
    out = toolkit._run_checkpoint(asset_name, bd, _suite_conteo(toolkit, 'x'), params, name='x', modo='sql')
    assert out.success is False
    assert out.failed == ['expect_table_row_count_to_be_between']


def test_table_asset_reusa_datasource(toolkit, sqlite_synapse):  # noqa: F811
    """Dos tablas de la misma base comparten el datasource SQL de la tarea."""
    toolkit._table_batch_definition(sqlite_synapse, 'a', 'main', 'ventas', None)
    toolkit._table_batch_definition(sqlite_synapse, 'b', 'main', 'ventas', None)
    names = [ds.name for ds in toolkit.context.data_sources.all().values()]
    assert names.count('t_synapse_bronze') == 1


def test_comprobar_api_gx_detecta_faltantes(monkeypatch):
    """Si GX renombra un privado, el humo falla con ImportError y no en silencio."""
    from great_expectations.datasource.fluent.sql_datasource import _SQLAsset

    from centraal_dataframework.gx_synapse import comprobar_api_gx

    monkeypatch.delattr(_SQLAsset, '_fully_specified_batch_requests')  # el método vive en la base de TableAsset
    with pytest.raises(ImportError, match='_fully_specified_batch_requests'):
        comprobar_api_gx()
