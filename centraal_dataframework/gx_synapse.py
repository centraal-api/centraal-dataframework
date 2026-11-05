"""Table asset de GX para Synapse serverless.

El ``TableAsset`` estándar de GX hace dos cosas que rompen la poda de particiones sobre vistas del lago:

- al registrarse ejecuta ``SELECT TOP 1 1 FROM <tabla>`` sin filtro (``test_connection``), que abre carpetas
  que la tarea no pidió;
- con un particionador, lista los valores distintos de la columna sobre toda la tabla (``param_defaults``)
  aunque el lote ya venga especificado.

``TableAssetSynapse`` anula las dos. La existencia del objeto la comprueba ``validate_table`` por catálogo antes
de registrar el asset. Depende de nombres privados de GX (ver ``NOMBRES_PRIVADOS_GX``): por eso
``great-expectations`` está acotado a la línea probada en ``pyproject.toml`` y ``comprobar_api_gx`` es la prueba
de humo que debe correr antes de subir esa cota.
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

NOMBRES_PRIVADOS_GX = ('_add_asset', '_fully_specified_batch_requests', '_effective_schema_name')


def comprobar_api_gx() -> None:
    """Lanza ``ImportError`` si la versión instalada de GX no expone los nombres de los que depende este módulo."""
    try:
        from great_expectations.datasource.fluent.sql_datasource import SQLDatasource, TableAsset, _NoMatchDiagnostics
    except ImportError as exc:  # pragma: no cover - depende de la versión de GX
        raise ImportError(f"great_expectations sin los nombres esperados por TableAssetSynapse: {exc}") from exc
    del _NoMatchDiagnostics
    faltantes = [
        nombre
        for nombre, clase in (
            ('_add_asset', SQLDatasource),
            ('_fully_specified_batch_requests', TableAsset),
            ('_effective_schema_name', TableAsset),
        )
        if not hasattr(clase, nombre)
    ]
    if faltantes:
        raise ImportError(
            f"great_expectations cambió nombres de los que depende TableAssetSynapse: {faltantes}. "
            "Revise la cota de versión en pyproject.toml."
        )


def _clase_table_asset():
    """Construye la subclase en el primer uso (GX se importa solo cuando hace falta)."""
    from great_expectations.datasource.fluent.sql_datasource import TableAsset, _NoMatchDiagnostics

    class TableAssetSynapse(TableAsset):
        """Table asset que no sondea la tabla ni lista valores distintos cuando el lote ya está especificado."""

        def test_connection(self) -> None:
            """No ejecuta SQL: ``validate_table`` ya comprobó por catálogo que el objeto existe."""
            return None

        def _fully_specified_batch_requests(self, batch_request):
            """Con todos los parámetros del particionador en ``options`` no hace falta listar candidatos."""
            if batch_request.partitioner is not None:
                particionador = self.get_partitioner_implementation(batch_request.partitioner)
                opciones = batch_request.options or {}
                if all(nombre in opciones for nombre in particionador.param_names):
                    return [batch_request], _NoMatchDiagnostics(candidate_count=1, offending_param=None)
            return super()._fully_specified_batch_requests(batch_request)

    return TableAssetSynapse


def registrar_table_asset(datasource: Any, name: str, schema: str, table: str) -> Any:
    """Registra en el datasource un ``TableAssetSynapse`` para ``schema.table`` y lo devuelve."""
    TableAssetSynapse = _clase_table_asset()
    asset = TableAssetSynapse(name=name, table_name=table, schema_name=schema)
    return datasource._add_asset(asset)


def batch_definition_tabla(asset: Any, particion: Optional[Tuple[str, Any]]) -> Tuple[Any, Dict[str, Any]]:
    """Batch definition del asset: tabla entera, o un valor de una columna con ``PartitionerColumnValue``.

    Devuelve ``(batch_definition, batch_parameters)``. ``validate_partitioner=False`` porque la validación interna
    de GX solo admite particionadores por fecha (``extract(year/month/day)``), justo los que impiden la poda.
    """
    nombre = f'{asset.name}_batch'
    if particion is None:
        return asset.add_batch_definition_whole_table(nombre), {}
    from great_expectations.core.partitioners import PartitionerColumnValue

    columna, valor = particion
    bd = asset.add_batch_definition(
        nombre, partitioner=PartitionerColumnValue(column_name=columna), validate_partitioner=False
    )
    return bd, {columna: valor}
