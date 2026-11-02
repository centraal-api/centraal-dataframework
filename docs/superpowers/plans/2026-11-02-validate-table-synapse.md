# validate_table en Synapse serverless con poda de particiones — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Que una tarea valide con GX, en modo `sql` sobre Synapse o en `pandas`, una tabla o vista con nombre, leyendo solo la partición que declara, y que `validate_query` en `sql` deje de dar falsos fallos de calidad.

**Architecture:** `GreatExpectationsToolKit.validate_table` verifica la partición contra la definición de la vista (`OBJECT_DEFINITION`, parser puro en `particiones.py`), registra un `TableAssetSynapse` (subclase de `TableAsset` de GX que no sondea la tabla ni lista valores distintos) con `PartitionerColumnValue`, y corre el checkpoint existente. Los errores técnicos de GX se convierten en `ErrorMotorCalidad`; `validate_query` en `sql` rechaza expectativas de columna antes de conectarse. GX queda acotado a `>=1.24,<1.25`.

**Tech Stack:** Python 3.10–3.14, great-expectations 1.24.x (fluent API), SQLAlchemy 2.x, pyodbc, pytest (SQLite en unitarias, marcador `synapse` en integración).

**Spec:** `docs/superpowers/specs/2026-11-02-validate-table-synapse-design.md`

## Global Constraints

- Estilo del repo: black `line-length = 120`, `skip-string-normalization` (comillas simples), isort, flake8 con docstrings google. Todo lo público lleva docstring en español, también en tests. `make test` en verde al cerrar cada tarea.
- `from __future__ import annotations`; sin `match`. Python `>=3.10,<3.15`.
- Textos, errores y logs en español. Por el log viajan métricas, no filas ni valores de datos.
- Cambio genérico: sin nombres de workspace, bases, esquemas, tablas ni rutas del cliente en código, tests ni docs. Los datos de prueba salen de `.env.synapse`.
- Sin tablas temporales (`#`) en el SQL que se ejecute en serverless. Sin fallback de motor: el `modo` pedido se ejecuta o se lanza error.
- `great-expectations = ">=1.24,<1.25"` en `pyproject.toml` (decisión del spec; subirlo es una decisión explícita que pasa por las pruebas de humo e integración).
- Lo que se cree o borre en Azure/Synapse lo ejecuta Felipe; el ejecutor del plan solo lee. No tocar `.env` ni `.env.synapse` más allá de leer nombres de variables.
- Commits pequeños por tarea, en la rama actual `feature/version_020`, **sin `Co-Authored-By`** y sin push (Felipe decide con Germán si se juntan antes del push).

## Review Focus

1. `particion={'fecha': valor}` con un valor con comillas, espacio, `;` o `--`: `ValueError` antes de tocar Synapse (Task 7, `test_validate_table_rejects_unsafe_partition_value`).
2. `particion` con dos columnas, o con dict vacío, o con un tipo que no es dict ni `'completa'`: `ValueError` claro (Task 7, `test_validate_table_particion_shape`).
3. Vista cuya columna de partición se llama distinto a la carpeta (`CAST(r.filepath(1) AS date) AS [dia]` sobre `fecha=*`): `ParticionInvalida` que nombra ambas (Task 2, `test_verificar_particion_nombre_de_carpeta_distinto`).
4. `tabla` con tres partes y base distinta a la del cliente, o con una sola parte, o con caracteres fuera de `[A-Za-z0-9_]`: `ValueError` sin conectarse (Task 3, `test_separar_tabla_*`).
5. GX devuelve `exception_info` en una expectativa (columna inexistente, Parquet dañado): el resultado no es `DQResult(success=False)` sino `ErrorMotorCalidad` con el mensaje de GX, en `sql` y en `pandas` (Task 4, `test_run_checkpoint_raises_on_metric_exception[sql|pandas]`).

---

### Task 1: Excepciones nuevas y cota de GX

**Files:**
- Modify: `centraal_dataframework/excepciones.py` (al final)
- Modify: `centraal_dataframework/__init__.py:7-34` (import y `__all__`)
- Modify: `pyproject.toml:37`
- Test: `tests/test_excepciones_dq.py` (nuevo)

**Interfaces:**
- Produces: `ParticionInvalida(Exception)` con `__init__(self, tabla: str, motivo: str)`, atributos `tabla`, `motivo`, `message = f"Partición inválida en {tabla!r}: {motivo}"`. `ErrorMotorCalidad(Exception)` con `__init__(self, name: str, modo: str, mensajes: list)`, atributos `name`, `modo`, `mensajes`, `message = f"GX no pudo evaluar {name!r} en modo {modo!r}: " + ' | '.join(mensajes)`. Ambas exportadas desde `centraal_dataframework`.

- [ ] **Step 1: Escribir el test que falla**

```python
"""Excepciones de partición y de motor de calidad."""
import pytest

def test_particion_invalida_message():
    from centraal_dataframework import ParticionInvalida
    exc = ParticionInvalida('bronze.vista', 'la columna fecha no sale de filepath()')
    assert exc.tabla == 'bronze.vista' and exc.motivo.startswith('la columna')
    assert str(exc) == "Partición inválida en 'bronze.vista': la columna fecha no sale de filepath()"

def test_error_motor_calidad_message():
    from centraal_dataframework import ErrorMotorCalidad
    exc = ErrorMotorCalidad('ventas', 'sql', ['a', 'b'])
    assert exc.mensajes == ['a', 'b'] and exc.modo == 'sql'
    assert str(exc) == "GX no pudo evaluar 'ventas' en modo 'sql': a | b"
```

- [ ] **Step 2: Correr y ver fallar** — Run: `.venv/bin/pytest tests/test_excepciones_dq.py -v` — Expected: FAIL `ImportError: cannot import name 'ParticionInvalida'`.
- [ ] **Step 3: Implementar las dos clases en `excepciones.py` y exportarlas en `__init__.py`** (import y entradas en `__all__`).
- [ ] **Step 4: Cambiar `pyproject.toml:37` a `great-expectations = ">=1.24,<1.25"`** y regenerar el lock: `pipx install poetry` (sin sudo) si no está, luego `poetry lock` en la raíz. `release.yml` corre `poetry install` y falla con el lock desactualizado. Verificar con `grep -A1 'name = "great-expectations"' poetry.lock` que la versión resuelta sigue en 1.24.x.
- [ ] **Step 5: Correr** `.venv/bin/pytest tests/test_excepciones_dq.py -v` — Expected: 2 PASS.
- [ ] **Step 6: Commit** — `git add centraal_dataframework/excepciones.py centraal_dataframework/__init__.py pyproject.toml poetry.lock tests/test_excepciones_dq.py && git commit -m "feat(dq): excepciones ParticionInvalida y ErrorMotorCalidad; GX acotado a 1.24.x"`

### Task 2: Parser puro de la definición de la vista

**Files:**
- Create: `centraal_dataframework/particiones.py`
- Test: `tests/test_particiones.py` (nuevo)

**Interfaces:**
- Produces, en `particiones.py`:
  - `columnas_filepath(definicion: str) -> Dict[str, int]`: columna → `n` de `filepath(n)`. Reconoce `r.filepath(1) AS fecha`, `CAST(r.filepath(1) AS date) AS [fecha]` y `CONVERT(date, r.filepath(1)) AS fecha`; alias con o sin corchetes; mayúsculas indiferentes; devuelve el nombre sin corchetes y en minúsculas.
  - `comodines_bulk(definicion: str) -> List[Optional[str]]`: segmentos con `*` de la ruta `BULK '...'`, en orden; para `fecha=*` devuelve `'fecha'`, para `*.parquet` devuelve `None`.
  - `verificar_particion(definicion: str, columna: str, tabla: str) -> None`: lanza `ParticionInvalida(tabla, motivo)` si la columna no está en `columnas_filepath` (motivo: `"la columna 'X' no sale de filepath(n) en la definición de la vista"`), si hay más de una columna de `filepath` (motivo: `"la vista tiene particiones anidadas [a, b]; este cambio admite una sola"`), o si el comodín `n` no es `<columna>=*` (motivo: `"filepath(n) corresponde a la carpeta 'Y=*', no a 'X=*'"`). Comparaciones en minúsculas.
  - `es_particionada(definicion: str) -> bool`: `bool(columnas_filepath(definicion))`.
- Consumes: `ParticionInvalida` (Task 1).

- [ ] **Step 1: Escribir los tests que fallan** (definiciones sintéticas como constantes del test; ninguna ruta real):

```python
DEF_SIMPLE = """CREATE VIEW demo.v AS
SELECT CAST(r.filepath(1) AS date) AS [fecha], r.*
FROM OPENROWSET(BULK 'capa/origen/tabla/fecha=*/*.parquet', DATA_SOURCE = 'Lago', FORMAT = 'PARQUET')
WITH ([id] bigint, [nombre] varchar(100)) AS r"""
DEF_SIN_CAST = DEF_SIMPLE.replace('CAST(r.filepath(1) AS date) AS [fecha]', 'r.filepath(1) AS fecha')
DEF_ANIDADA = DEF_SIMPLE.replace("AS [fecha], r.*", "AS [anio], r.filepath(2) AS [mes], r.*").replace('fecha=*/*.parquet', 'anio=*/mes=*/*.parquet')
DEF_ALIAS_DISTINTO = DEF_SIMPLE.replace('AS [fecha]', 'AS [dia]')
DEF_SIN_FILEPATH = """CREATE VIEW demo.v AS SELECT id, nombre FROM demo.tabla_externa"""

def test_columnas_filepath_simple(): assert columnas_filepath(DEF_SIMPLE) == {'fecha': 1}
def test_columnas_filepath_sin_cast(): assert columnas_filepath(DEF_SIN_CAST) == {'fecha': 1}
def test_columnas_filepath_anidada(): assert columnas_filepath(DEF_ANIDADA) == {'anio': 1, 'mes': 2}
def test_columnas_filepath_ninguna(): assert columnas_filepath(DEF_SIN_FILEPATH) == {}
def test_comodines_bulk(): assert comodines_bulk(DEF_SIMPLE) == ['fecha', None]
def test_es_particionada(): assert es_particionada(DEF_SIMPLE) and not es_particionada(DEF_SIN_FILEPATH)
def test_verificar_particion_ok(): verificar_particion(DEF_SIMPLE, 'FECHA', 't')  # no lanza
def test_verificar_particion_columna_no_filepath():
    with pytest.raises(ParticionInvalida, match="no sale de filepath"): verificar_particion(DEF_SIMPLE, 'id', 't')
def test_verificar_particion_anidada():
    with pytest.raises(ParticionInvalida, match='anidadas'): verificar_particion(DEF_ANIDADA, 'anio', 't')
def test_verificar_particion_nombre_de_carpeta_distinto():
    with pytest.raises(ParticionInvalida, match="'fecha=\\*', no a 'dia=\\*'"): verificar_particion(DEF_ALIAS_DISTINTO, 'dia', 't')
```

- [ ] **Step 2: Correr y ver fallar** — Run: `.venv/bin/pytest tests/test_particiones.py -v` — Expected: FAIL `ModuleNotFoundError: centraal_dataframework.particiones`.
- [ ] **Step 3: Implementar `particiones.py`** con regex: columnas `re.compile(r"filepath\(\s*(\d+)\s*\)[^,]*?\bAS\s+\[?(\w+)\]?", re.I | re.S)` aplicada a la lista de selección; ruta `re.compile(r"BULK\s*'([^']+)'", re.I)`.
- [ ] **Step 4: Correr** `.venv/bin/pytest tests/test_particiones.py -v` — Expected: 10 PASS.
- [ ] **Step 5: Commit** — `git add centraal_dataframework/particiones.py tests/test_particiones.py && git commit -m "feat(dq): parser de particiones filepath() en definiciones de vistas"`

### Task 3: Catálogo en `SynapseClient` y separación de nombres

**Files:**
- Modify: `centraal_dataframework/synapse.py` (después de `render`, y métodos en `SynapseClient` junto a `data_processed_mb`)
- Test: `tests/test_synapse.py` (añadir al final; usa las fixtures `fake_odbc` y `client` existentes y `FakePyodbc.results` si existe, si no, `monkeypatch.setattr(client, 'scalar', ...)`)

**Interfaces:**
- Produces:
  - `separar_tabla(tabla: str, database: Optional[str]) -> Tuple[str, str]` (función de módulo en `synapse.py`): acepta `esquema.tabla` y `base.esquema.tabla`; cada parte debe cumplir `^[A-Za-z_][A-Za-z0-9_]*$` (sin corchetes); con tres partes, `base` debe ser igual a `database` (sin distinguir mayúsculas) o `ValueError("la base 'X' no coincide con la del cliente 'Y'; use synapse_for('X')")`; con una parte, `ValueError("indique esquema.tabla")`.
  - `SynapseClient.tipo_objeto(self, tabla: str) -> Optional[str]`: `SELECT type FROM sys.objects WHERE object_id = OBJECT_ID(?)` con `tabla` como `esquema.tabla`; devuelve `'V'`, `'U'`, etc. sin espacios, o `None`.
  - `SynapseClient.definicion_objeto(self, tabla: str) -> Optional[str]`: `SELECT OBJECT_DEFINITION(OBJECT_ID(?))`.
- Consumes: nada nuevo.

- [ ] **Step 1: Tests que fallan**

```python
def test_separar_tabla_dos_partes(): assert separar_tabla('bronze_x.vista', 'bronze') == ('bronze_x', 'vista')
def test_separar_tabla_tres_partes_misma_base(): assert separar_tabla('Bronze.s.v', 'bronze') == ('s', 'v')
def test_separar_tabla_base_distinta():
    with pytest.raises(ValueError, match="no coincide"): separar_tabla('silver.s.v', 'bronze')
def test_separar_tabla_una_parte():
    with pytest.raises(ValueError, match='esquema.tabla'): separar_tabla('v', 'bronze')
def test_separar_tabla_caracteres():
    with pytest.raises(ValueError): separar_tabla('s.v; DROP', 'bronze')
def test_tipo_y_definicion_objeto(fake_odbc, client):  # fake devuelve 'V ' y luego un texto
    ...assert client.tipo_objeto('s.v') == 'V'; assert 'OPENROWSET' in client.definicion_objeto('s.v')
    ...assert fake_odbc.executed[...] contiene 'sys.objects' y 'OBJECT_DEFINITION' con el parámetro ('s.v',)
```

- [ ] **Step 2: Correr y ver fallar** — Run: `.venv/bin/pytest tests/test_synapse.py -k "separar_tabla or objeto" -v` — Expected: FAIL `ImportError: separar_tabla`.
- [ ] **Step 3: Implementar** `separar_tabla`, `tipo_objeto`, `definicion_objeto`.
- [ ] **Step 4: Correr** el mismo comando — Expected: PASS. Luego `.venv/bin/pytest tests/test_synapse.py -v` — Expected: todo PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(synapse): separar_tabla, tipo_objeto y definicion_objeto"`

### Task 4: Errores técnicos de GX → `ErrorMotorCalidad`

**Files:**
- Modify: `centraal_dataframework/resources.py:264-302` (`_run_checkpoint`) y `:346-357` (junto a `_failed_expectations`)
- Test: `tests/test_resources_gx.py`

**Interfaces:**
- Produces: `_raised_exceptions(result: Any) -> List[str]` (función de módulo): recorre `run_results` → `results` → `exception_info` (dict de dicts, o dict plano con `raised_exception`) y devuelve los `exception_message` con `raised_exception` verdadero, sin duplicados, en orden. `_run_checkpoint(..., name: str = '', modo: str = '')` gana dos parámetros con valor por defecto y, si `_raised_exceptions(result)` no está vacío, lanza `ErrorMotorCalidad(name or asset_name, modo or 'pandas', mensajes)` en vez de devolver `DQResult`. `validate_query` y `_run_suite_on_df` pasan `name` y `modo`.
- Consumes: `ErrorMotorCalidad` (Task 1).

- [ ] **Step 1: Tests que fallan** (en SQLite una columna inexistente produce `exception_info` en GX):

```python
@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_run_checkpoint_raises_on_metric_exception(toolkit, sqlite_synapse, modo):
    """Un error técnico de GX (columna inexistente) es ErrorMotorCalidad, no un fallo de calidad."""
    import great_expectations as gx
    from centraal_dataframework import ErrorMotorCalidad
    exps = [gx.expectations.ExpectTableRowCountToBeBetween(min_value=1)] if modo == 'sql' else []
    exps.append(gx.expectations.ExpectColumnValuesToNotBeNull(column='no_existe'))
    with pytest.raises(ErrorMotorCalidad) as info:
        toolkit.validate_query(sqlite_synapse, 'ventas', QUERY, exps, params={'fecha': '2026-11-01'}, modo=modo)
    assert info.value.modo == modo and info.value.mensajes
```

  Nota: en modo `sql` este test se escribe ahora y se mantiene cuando la Task 5 restrinja `validate_query`; en esa tarea se cambia a usar `validate_table` (Task 7) con una definición no particionada. Dejar un comentario `# TODO Task 7` en el test.

- [ ] **Step 2: Correr y ver fallar** — Run: `.venv/bin/pytest tests/test_resources_gx.py -k metric_exception -v` — Expected: FAIL (`DQResult` devuelto, no excepción).
- [ ] **Step 3: Implementar** `_raised_exceptions` y la lanzada en `_run_checkpoint`.
- [ ] **Step 4: Correr** `.venv/bin/pytest tests/test_resources_gx.py -v` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(dq): ErrorMotorCalidad cuando GX no puede evaluar una expectativa"`

### Task 5: `validate_query` en `sql` rechaza expectativas de columna

**Files:**
- Modify: `centraal_dataframework/resources.py:169-221` (`validate_query`) y helper nuevo junto a `_build_suite`
- Test: `tests/test_resources_gx.py:170-216` (los tests `sql` actuales de `validate_query` con columnas cambian)

**Interfaces:**
- Produces: `_expectativas_de_columna(suite: Any) -> List[str]`: tipos de las expectativas cuya `configuration.kwargs` (o `.kwargs`) tiene alguna de `column`, `column_list`, `column_A`, `column_B`, `column_set`. `validate_query` en `modo='sql'` lanza, antes de `render` y de cualquier conexión, `ValueError("validate_query en modo 'sql' no admite expectativas de columna [tipos]: use validate_table(..., modo='sql') o modo='pandas' (las filas de la consulta viajan a la Function)")`.
- Consumes: nada nuevo.

- [ ] **Step 1: Tests que fallan y ajuste de los existentes**
  - Nuevo: `test_validate_query_sql_rejects_column_expectations(toolkit, sqlite_synapse)`: `pytest.raises(ValueError, match='validate_table')` y `sqlite_synapse.kwargs_calls == 0` y `sqlite_synapse.queries == []`.
  - Nuevo: `test_validate_query_sql_allows_table_expectations(toolkit, sqlite_synapse)`: solo `ExpectTableRowCountToBeBetween(min_value=1)` en `sql` → `success` True, `read_df` no llamado.
  - Cambiar `test_validate_query_success_in_both_modes`, `test_validate_query_reports_failures_in_both_modes`, `test_validate_query_sql_mode_does_not_pull_rows`, `test_validate_query_with_existing_suite`, `test_validate_query_measures_data_processed`, `test_validate_query_without_cost_measure_has_none`, `test_validate_query_twice_reuses_the_sql_datasource`: donde usen `modo='sql'` con `_expectations()`, usar `_expectations_tabla()` (solo conteo) o `modo='pandas'`, conservando lo que cada test prueba (p. ej. "no trae filas" sigue en `sql` con conteo). El test de la Task 4 en `sql` pasa a `pytest.skip` temporal con el `# TODO Task 7`.
- [ ] **Step 2: Correr y ver fallar** — Run: `.venv/bin/pytest tests/test_resources_gx.py -v` — Expected: los dos nuevos FAIL.
- [ ] **Step 3: Implementar** `_expectativas_de_columna` y la comprobación en `validate_query`; actualizar el docstring de `validate_query` (pandas: las filas viajan a la Function; sql: solo expectativas de tabla; cruces por tabla → `validate_table`).
- [ ] **Step 4: Correr** `.venv/bin/pytest tests/test_resources_gx.py -v` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(dq): validate_query en sql rechaza expectativas de columna con error claro"`

### Task 6: `TableAssetSynapse` y el batch definition por tabla

**Files:**
- Create: `centraal_dataframework/gx_synapse.py`
- Modify: `centraal_dataframework/resources.py` (nuevo método `_table_batch_definition` junto a `_query_batch_definition`)
- Test: `tests/test_gx_synapse.py` (nuevo)

**Interfaces:**
- Produces, en `gx_synapse.py`:
  - `class TableAssetSynapse(TableAsset)`: `test_connection(self) -> None` no ejecuta SQL (docstring: la existencia la comprueba `validate_table` por catálogo antes de registrar el asset); `_fully_specified_batch_requests(self, batch_request)` devuelve `([batch_request], _NoMatchDiagnostics(candidate_count=1, offending_param=None))` cuando `batch_request.partitioner` no es `None` y todos los `param_names` del particionador están en `batch_request.options`; si no, delega en `super()`.
  - `registrar_table_asset(datasource: Any, name: str, schema: str, table: str) -> Any`: construye `TableAssetSynapse(name=name, table_name=table, schema_name=schema)` y lo registra con `datasource._add_asset(asset)`.
  - `batch_definition_tabla(asset: Any, particion: Optional[Tuple[str, Any]]) -> Tuple[Any, Dict[str, Any]]`: sin partición → `asset.add_batch_definition_whole_table(f'{asset.name}_batch')`, `{}`; con `(columna, valor)` → `asset.add_batch_definition(f'{asset.name}_batch', partitioner=PartitionerColumnValue(column_name=columna), validate_partitioner=False)`, `{columna: valor}`.
  - `NOMBRES_PRIVADOS_GX = ('_add_asset', '_fully_specified_batch_requests', '_effective_schema_name')` y `comprobar_api_gx() -> None`, que lanza `ImportError` con mensaje si falta alguno de esos atributos en `SQLDatasource`/`TableAsset` o no se importa `_NoMatchDiagnostics`.
- En `resources.py`: `GreatExpectationsToolKit._table_batch_definition(self, synapse: Any, name: str, schema: str, table: str, particion: Optional[Tuple[str, Any]]) -> Tuple[str, Any, Dict[str, Any]]` que reutiliza el `add_or_update_sql` de `_query_batch_definition` (extraer `_sql_datasource(self, synapse) -> Any`) y devuelve `(asset_name, batch_def, batch_parameters)`.
- Consumes: nada de tareas previas salvo convenciones.

- [ ] **Step 1: Tests que fallan** (SQLite; capturar el SQL con un `logging.Handler` sobre `sqlalchemy.engine` como en los spikes):

```python
def test_comprobar_api_gx_no_lanza(): comprobar_api_gx()
def test_table_asset_sin_sonda_ni_distinct(toolkit, sqlite_synapse, caplog):
    """Con partición especificada no hay SELECT DISTINCT ni LIMIT 1 de prueba; sí WHERE fecha = ?."""
    ...asset_name, bd, params = toolkit._table_batch_definition(sqlite_synapse, 'v', 'main', 'ventas', ('fecha', '2026-11-01'))
    ...out = toolkit._run_checkpoint(asset_name, bd, suite_conteo, params, name='v', modo='sql')
    sql = '\n'.join(r.getMessage() for r in caplog.records if r.name.startswith('sqlalchemy.engine')).lower()
    assert 'distinct' not in sql and 'limit 1' not in sql and 'where fecha = ?' in sql and out.success
def test_table_asset_whole_table(toolkit, sqlite_synapse, caplog):  # sin partición: WHERE 1 = 1, sin distinct
def test_table_asset_particion_inexistente_no_valida_en_vacio(toolkit, sqlite_synapse):
    """Un valor sin filas da row_count 0: la expectativa de conteo falla, no pasa en vacío."""
```

  Nota SQLite: el esquema por defecto se llama `main`; `TableAssetSynapse` debe aceptar `schema_name='main'` ahí. En SQLite el `expect_table_row_count_to_be_between(min_value=1)` con partición inexistente devuelve `success=False` (0 filas).

- [ ] **Step 2: Correr y ver fallar** — Run: `.venv/bin/pytest tests/test_gx_synapse.py -v` — Expected: FAIL `ModuleNotFoundError`.
- [ ] **Step 3: Implementar** `gx_synapse.py` y `_table_batch_definition`/`_sql_datasource` en `resources.py`.
- [ ] **Step 4: Correr** `.venv/bin/pytest tests/test_gx_synapse.py tests/test_resources_gx.py -v` — Expected: PASS.
- [ ] **Step 5: Commit** — `git add centraal_dataframework/gx_synapse.py centraal_dataframework/resources.py tests/test_gx_synapse.py && git commit -m "feat(dq): TableAssetSynapse sin sonda ni DISTINCT, batch por tabla con PartitionerColumnValue"`

### Task 7: `validate_table`

**Files:**
- Modify: `centraal_dataframework/resources.py` (nuevo método público después de `validate_query`)
- Modify: `tests/test_resources_gx.py:96-130` (`_SqliteSynapse` gana `definicion`, `tipo` y `read_df(sql, *params)`)
- Test: `tests/test_resources_gx.py` (nuevos tests) y el `# TODO Task 7` de la Task 4

**Interfaces:**
- Produces: `GreatExpectationsToolKit.validate_table(self, synapse: Any, name: str, tabla: str, expectations: Optional[Sequence[Any]] = None, *, suite_name: Optional[str] = None, particion: Union[None, str, Dict[str, Any]] = None, modo: str = 'sql', result_format: str = DEFAULT_RESULT_FORMAT, medir_costo: bool = False, public: bool = False) -> DQResult`. Orden de comprobaciones, todas antes de crear datasources:
  1. `modo in MODOS` o `ValueError`; `expectations` o `suite_name` o `ValueError`.
  2. `schema, table = separar_tabla(tabla, getattr(synapse, 'database', None))`.
  3. Forma de `particion`: `None`, la cadena `'completa'`, o dict de exactamente una entrada `{columna: valor}` con `columna` que cumpla `^[A-Za-z_][A-Za-z0-9_]*$` y `str(valor)` que cumpla `_SAFE_VALUE_RE`; si no, `ValueError` que diga la forma esperada.
  4. `tipo = synapse.tipo_objeto(f'{schema}.{table}')`; `None` → `ParticionInvalida(tabla, "el objeto no existe o la identidad no puede verlo")`.
  5. Si `tipo == 'V'`: `definicion = synapse.definicion_objeto(...)`; `None` → `ParticionInvalida(tabla, "no se puede leer la definición de la vista: la identidad necesita GRANT VIEW DEFINITION además de db_datareader")`. Con dict: `verificar_particion(definicion, columna, tabla)`. Con `None`: si `es_particionada(definicion)` → `ParticionInvalida(tabla, "la vista está particionada por [cols]; declare particion={'col': valor} o particion='completa'")`. Con `'completa'`: pasa.
  6. Si `tipo != 'V'`: con dict → `ParticionInvalida(tabla, "no es una vista con filepath(): no tiene particiones que aplicar")`; `None`/`'completa'` → tabla entera.
  7. `modo == 'pandas'`: `sql = f'SELECT * FROM {schema}.{table}'` más `f" WHERE {columna} = '{valor}'"` si hay dict (valor ya validado por `_SAFE_VALUE_RE`), `df = synapse.read_df(sql)`, `_run_suite_on_df(df, name, suite, public, result_format)`.
  8. `modo == 'sql'`: `_table_batch_definition(...)` + `_run_checkpoint(asset_name, batch_def, suite, params, public, result_format, name=name, modo='sql')`.
  9. `out.modo = modo`; `medir_costo` como en `validate_query`; `logger.info("GX %s [%s] tabla=%s particion=%s success=%s data_processed_mb=%s", ...)` donde `particion` se loguea como la columna (no el valor).
- `_SqliteSynapse.__init__(self, path, processed=(100.0, 112.5), definicion=None, tipo='V')` y métodos `tipo_objeto(tabla) -> self.tipo`, `definicion_objeto(tabla) -> self.definicion`, `read_df(sql, *params)` que registra `sql` en `queries`.
- Consumes: Tasks 1–6.

- [ ] **Step 1: Tests que fallan** (usar `DEF_SIMPLE` y `DEF_SIN_FILEPATH` importados de `tests/test_particiones.py`, con la tabla `ventas` de SQLite y esquema `main`):

```python
def _sqlite_particionado(tmp_path): return _SqliteSynapse(path, definicion=DEF_SIMPLE)  # vista con fecha=*
@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_validate_table_particion_ok(toolkit, modo): ... particion={'fecha': '2026-11-01'} → success True, failed == []
@pytest.mark.parametrize('modo', ['sql', 'pandas'])
def test_validate_table_particion_reports_failures(toolkit, modo): ... '2026-10-31' → failed == ['expect_column_values_to_not_be_null']
def test_validate_table_sql_does_not_pull_rows(toolkit): ... queries == [] y kwargs_calls == 1
def test_validate_table_pandas_reads_filtered_select(toolkit): ... queries == ["SELECT * FROM main.ventas WHERE fecha = '2026-11-01'"]
def test_validate_table_sin_particion_en_vista_particionada(toolkit): pytest.raises(ParticionInvalida, match='completa')
def test_validate_table_completa(toolkit): particion='completa' → success False (hay un nulo en el total), sin excepción
def test_validate_table_columna_no_particion(toolkit): particion={'id': 1} → ParticionInvalida 'no sale de filepath'
def test_validate_table_definicion_ilegible(toolkit): definicion=None, tipo='V' → ParticionInvalida match 'VIEW DEFINITION'
def test_validate_table_objeto_inexistente(toolkit): tipo=None → ParticionInvalida match 'no existe'
def test_validate_table_tabla_externa_entera(toolkit): tipo='U', particion=None → valida entera
def test_validate_table_tabla_externa_con_particion(toolkit): tipo='U', particion={'fecha': ...} → ParticionInvalida 'no es una vista'
def test_validate_table_rejects_unsafe_partition_value(toolkit): {'fecha': "2026-11-01' OR 1=1"} → ValueError, kwargs_calls == 0
def test_validate_table_particion_shape(toolkit): {'a': 1, 'b': 2}, {}, 'otra', 5 → ValueError
def test_validate_table_base_distinta(toolkit): tabla='silver.main.ventas' → ValueError 'no coincide'
def test_validate_table_measures_data_processed(toolkit): medir_costo=True → data_processed_mb == 12.5
def test_run_checkpoint_raises_on_metric_exception[sql] (Task 4): ahora con validate_table y columna 'no_existe' → ErrorMotorCalidad
```

- [ ] **Step 2: Correr y ver fallar** — Run: `.venv/bin/pytest tests/test_resources_gx.py -k validate_table -v` — Expected: FAIL `AttributeError: validate_table`.
- [ ] **Step 3: Implementar `validate_table`** con el orden de comprobaciones de arriba y los textos de error indicados. Docstring: caso principal de la plataforma; bronze con `particion`, silver/gold sin; pandas trae las filas a la Function.
- [ ] **Step 4: Correr** `.venv/bin/pytest tests/test_resources_gx.py tests/test_gx_synapse.py -v` — Expected: PASS. Luego `make test` — Expected: formato sin cambios, flake8 limpio, pytest en verde (los `synapse` se saltan sin `.env.synapse`; con él, se corren en la Task 8).
- [ ] **Step 5: Commit** — `git commit -am "feat(dq): validate_table con verificación de partición por definición de la vista"`

### Task 7b: El arranque en frío de serverless (42109) es transitorio

**Files:**
- Modify: `centraal_dataframework/synapse.py:31` (`TRANSIENT_CODES`)
- Test: `tests/test_synapse.py` (junto a los tests de reintento existentes, con `fake_odbc` y `client`)

**Interfaces:**
- Produces: `TRANSIENT_CODES` incluye `'42109'` (`The SQL pool is warming up. Please try again.`). Sin cambios de firma.

- [ ] **Step 1: Test que falla**

```python
def test_sql_pool_warming_up_is_retried(fake_odbc, client):
    """42109 (pool en frío) se reintenta como transitorio y la segunda conexión responde."""
    fake_odbc.fail_connect_once(FakeOperationalError('HY000', '[HY000] ... The SQL pool is warming up. Please try again. (42109) (SQLDriverConnect)'))
    assert client.scalar('SELECT 1') == 1
    assert ('sleep', 1) in fake_odbc.executed or any(e[0] == 'sleep' for e in fake_odbc.executed)
```

  Si `FakePyodbc` no tiene un mecanismo para fallar la conexión una vez, añadirlo al fake del test (p. ej. una cola `connect_errors`), siguiendo cómo el test de reintentos actual simula el error transitorio.

- [ ] **Step 2: Correr y ver fallar** — Run: `.venv/bin/pytest tests/test_synapse.py -k warming -v` — Expected: FAIL (la excepción se propaga sin reintento).
- [ ] **Step 3: Añadir `'42109'` a `TRANSIENT_CODES`** y una línea en el docstring de `_is_transient`.
- [ ] **Step 4: Correr** `.venv/bin/pytest tests/test_synapse.py -v` — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "fix(synapse): reintentar 42109 (pool en frío de serverless)"`

### Task 8: Pruebas de integración con la trampa y variables de ejemplo

**Files:**
- Modify: `tests/test_synapse_integration.py:70-100` (reescribir `test_gx_native_sql_matches_pandas`) y añadir tests
- Modify: `docs/synapse.env.example` (tres variables nuevas al final, descritas en genérico)

**Interfaces:**
- Consumes: `validate_table`, `ParticionInvalida`, `ErrorMotorCalidad`, variables `SYNAPSE_TEST_PARTITION_TABLE`, `SYNAPSE_TEST_PARTITION_COLUMN`, `SYNAPSE_TEST_PARTITION_VALUE` (ya en `.env.synapse` de Felipe; la vista tiene dos fotos sanas y una carpeta con un Parquet dañado).
- Produces: fixture `particion()` que devuelve `(tabla, columna, valor)` o `pytest.skip`.

- [ ] **Step 1: Escribir los tests** (todos `-m synapse`; imprimir el SQL generado con `caplog` sobre `sqlalchemy.engine` como hoy):
  - `test_gx_query_sql_with_columns_is_rejected(client)`: `validate_query(..., modo='sql')` con la suite actual → `ValueError` match `validate_table`, y la misma en `pandas` → `success` True.
  - `test_validate_table_particion_sql_matches_pandas(client, caplog, particion)`: suite = conteo ≥1, `ExpectColumnToExist(column=columna)`, `ExpectColumnValuesToNotBeNull(column=columna)`, `ExpectColumnValuesToBeNull(column=columna)` (forzada, debe fallar) en `sql` y `pandas`; `failed == ['expect_column_values_to_be_null']` en ambos; el SQL impreso contiene `where <columna> = ?`, no contiene `distinct`, ` #`, `top 1 1`, ni `year(`/`month(`/`day(`.
  - `test_validate_table_completa_falla_por_trampa(client, particion)`: `particion='completa'`, `modo='sql'` → `pytest.raises(ErrorMotorCalidad)` y `'parquet' in str(exc).lower()`.
  - `test_validate_table_sin_particion_se_niega(client, particion)`: → `ParticionInvalida` match `completa`.
  - `test_validate_table_columna_que_no_es_particion(client, particion)`: `particion={<otra columna de la vista>: 1}` (tomar la primera columna distinta de la partición con `SELECT TOP 0 *`) → `ParticionInvalida` match `filepath`.
- [ ] **Step 2: Correr** `.venv/bin/pytest -m synapse -v -s tests/test_synapse_integration.py` — Expected: todos PASS (si aparece `42109 ... warming up`, repetir; es un defecto conocido anotado aparte).
- [ ] **Step 3: Añadir a `docs/synapse.env.example`** las tres variables con comentario genérico (vista de prueba con una partición dañada para demostrar la poda; columna; valor de una foto sana), y reemplazar el ejemplo de `SYNAPSE_TEST_GX_QUERY` por uno sin nombres del cliente (`SELECT id, nombre FROM esquema.vista WHERE fecha = '2026-01-31'`).
- [ ] **Step 4: Commit** — `git add tests/test_synapse_integration.py docs/synapse.env.example && git commit -m "test(synapse): integración de validate_table con trampa de poda"`

### Task 9: Documentación, notebooks, checklist y CHANGELOG

**Files:**
- Modify: `README.md:161-165` (bloque "Calidad sin traer los datos") y tabla de síntomas (+ fila `VIEW DEFINITION`), `:191` (fila del notebook 02)
- Modify: `docs/02_great_expectations.ipynb` celdas 10, 11, 12; `docs/03_synapse.ipynb` celdas 12, 13 (con `NotebookEdit`; celda de configuración: leer `SYNAPSE_TEST_PARTITION_*`)
- Modify: `docs/CHECKLIST_PRUEBAS.md:18-19, 25, 27-30, 58-61, 74, 78-81, 98, 117` y bloque 4
- Modify: `docs/datalake.env.example:6` (comentar `AZURE_STORAGE_CONNECTION_STRING=`), `docs/PRUEBA_EN_FUNCTION.md` §3 (GRANT), `docs/MOTOR_DE_COMPUTO.md` (si menciona `validate_query` en sql con columnas)
- Modify: `CHANGELOG.md` `[Unreleased]`

- [ ] **Step 1: README** — `validate_table(synapse, name, 'esquema.vista', expectations, particion={'fecha': valor}, modo='sql')` como caso principal; bronze con partición, silver/gold sin; la regla "vista particionada sin `particion` → `ParticionInvalida`, `particion='completa'` para toda la vista"; `validate_query` para cruces en `pandas` (las filas viajan a la Function) y en `sql` solo expectativas de tabla; `ErrorMotorCalidad`; requisito `GRANT VIEW DEFINITION` (fila nueva en la tabla de síntomas con el mensaje de `ParticionInvalida`); restricción: la vista que se valida debe ser la que define el `OPENROWSET` (no se siguen vistas sobre vistas) y una sola columna de partición por llamada; nota de la cota de GX y por qué.
- [ ] **Step 2: Notebook 02 §5** — celda 10: texto nuevo (tabla con nombre + partición; cruces en pandas); celda 11: `validar_foto(ctx)` con `validate_table(ctx.synapse, 'foto', TABLA_PARTICION, EXPECTATIVAS_FOTO, particion={COL: VAL}, modo=params.modo, medir_costo=True)` donde `EXPECTATIVAS_FOTO = [conteo ≥ 1, NotBeNull(COL)]`; correr `sql` y `pandas` e imprimir `success | fallidas | MB`; celda 12: (a) fallo forzado `ExpectColumnValuesToBeNull(column=COL)` en `sql` → `fallidas: ['expect_column_values_to_be_null']`; (b) `validate_table` sin `particion` → imprime el `ParticionInvalida`; (c) `validate_query` del `QUERY_VENTAS` (VALUES) en `pandas` → `success: True`, y en `sql` → imprime el `ValueError`. Todo bajo `if HAY_SYNAPSE and TABLA_PARTICION`, con `SALTADO` si falta.
- [ ] **Step 3: Notebook 03 §6** — celda 12: texto nuevo; celda 13: `dq_tabla(ctx)` con `validate_table` y partición, en `sql` y `pandas`, más el ejemplo `validate_query` de `GX_QUERY` en `pandas`.
- [ ] **Step 4: Ejecutar los dos notebooks** desde `docs/` con `jupyter nbconvert --to notebook --execute --allow-errors --output-dir <scratch>` y comprobar en las salidas los textos esperados del Step 2 y 3. Luego dejar los `.ipynb` del repo sin salidas (`jupyter nbconvert --clear-output --inplace`).
- [ ] **Step 5: CHECKLIST** — bloque 0: variables `SYNAPSE_TEST_PARTITION_*` y `GRANT VIEW DEFINITION`; bloque 1: `make test` corre las pruebas de Synapse si hay `.env.synapse` (quitar "se saltan"), lista de tests nueva; bloque 2: ítems de 02 §5 y 03 §6 con `validate_table`; "Poda de particiones" pasa a describir la trampa (partición sana pasa, `completa` falla con error de Parquet); bloque 4: `GRANT VIEW DEFINITION` junto al `CREATE USER`, y la tarea de verificación con `validate_table(..., modo='sql')`.
- [ ] **Step 6: `datalake.env.example`** — comentar la línea `AZURE_STORAGE_CONNECTION_STRING=` y explicar que vacía rompe la identidad. `PRUEBA_EN_FUNCTION.md` §3: añadir `GRANT VIEW DEFINITION TO [<app>];` al bloque SQL. `MOTOR_DE_COMPUTO.md`: alinear con `validate_table` (caso principal) y `validate_query` en pandas, y reemplazar el ejemplo de la línea 89 que nombra una tabla del cliente por `esquema.vista`.
- [ ] **Step 7: CHANGELOG `[Unreleased]`** — Added: `validate_table` (+ partición, `ParticionInvalida`, `'completa'`), `ErrorMotorCalidad`, `TableAssetSynapse`, `separar_tabla`/`tipo_objeto`/`definicion_objeto`, parser `particiones.py`, variables de prueba nuevas. Changed: `validate_query` en `sql` rechaza expectativas de columna; `great-expectations` acotado a `>=1.24,<1.25` y por qué. Fixed: ejemplo de `.env` con la connection string comentada; `SynapseClient` reintenta 42109 (pool en frío).
- [ ] **Step 8: Verificar** `make test` en verde y `grep -rnEif .superpowers/sdd/nombres-cliente.txt README.md docs/*.md docs/*.ipynb docs/superpowers centraal_dataframework tests CHANGELOG.md` sin resultados (el patrón vive fuera del repo porque contiene nombres del cliente).
- [ ] **Step 9: Commit** — `git add README.md docs CHANGELOG.md && git commit -m "docs: validate_table, trampa de poda, GRANT VIEW DEFINITION y cota de GX"`

### Task 10: Repetir los bloques 1 y 2 del checklist y registrar

**Files:**
- Modify: `docs/superpowers/runs/2026-11-02-checklist-pruebas.md` (sección nueva "Corrida 3, con validate_table")

- [ ] **Step 1:** `make test` con `.venv` en el PATH — Expected: isort/black sin cambios, flake8 limpio, pytest todo en verde incluidas las `synapse` (repetir una vez si sale `42109`).
- [ ] **Step 2:** `pytest -m synapse -v -s tests/test_synapse_integration.py` — Expected: PASS; copiar al registro el SQL generado (saneado) y la evidencia de la trampa.
- [ ] **Step 3:** Ejecutar los notebooks 01, 02 y 03 headless y comprobar cada ítem del checklist (bloque 2) como en la corrida 2; anotar PASA/FALLA con evidencia saneada.
- [ ] **Step 4:** Añadir la sección al registro; `git add docs/superpowers/runs && git commit -m "docs(runs): corrida 3 del checklist con validate_table"`.
