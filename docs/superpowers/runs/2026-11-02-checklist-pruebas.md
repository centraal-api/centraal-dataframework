# Corrida del checklist de pruebas — 2026-11-02

Plan: [CHECKLIST_PRUEBAS.md](../../CHECKLIST_PRUEBAS.md). Alcance: bloques 0 a 2. Los bloques 3 y 4 quedan fuera de esta corrida por decisión de Felipe.

Destino: workspace serverless de desarrollo del cliente (datos del destino fuera del repo). En Azure solo lectura.

Convención: cada ítem lleva `PASA`, `FALLA`, `SALTADO` o `PENDIENTE`, con la evidencia (salida real) resumida. Nada se marca sin evidencia.

## 0. Preparación

- PASA — Librería instalada con el extra: `.venv` (Python 3.13.5) con `pip install -e ".[synapse]"` más jupyter, python-dotenv y herramientas de `make test`. Evidencia: cdf 0.2.0, pyodbc 5.3.0, sqlalchemy 2.1.6, gx 1.24.1.
- PENDIENTE — Driver ODBC 18: `pyodbc.drivers()` devolvió `[]`. Requiere sudo (instalación a cargo de Felipe).
- PASA — `az login`: usuario con acceso al workspace, `az synapse workspace show` devuelve el workspace, estado `Succeeded`, endpoint `<workspace>-ondemand.sql.azuresynapse.net`.
- PASA — `.env` existe con: AZURE_STORAGE_CONNECTION_STRING, STORAGE_ACCOUNT_NAME, DATALAKE_TEST_PATH, CONTENEDOR_VALIDACIONES (valores no leídos).
- PASA — `.env.synapse` existe con: SYNAPSE_WORKSPACE, SYNAPSE_DATABASE, SYNAPSE_AUTH, SYNAPSE_QUERY_TIMEOUT, SYNAPSE_ODBC_DRIVER, SYNAPSE_TEST_TABLE, SYNAPSE_TEST_OTHER_DB, SYNAPSE_TEST_DDL_SCHEMA, SYNAPSE_TEST_GX_QUERY, SYNAPSE_TEST_GX_COLUMN (valores no leídos).
- SALTADO — IP en firewall y `CREATE USER`: ya hecho por Felipe (admin de Entra del workspace), según su indicación del 2026-11-02.

## 1. Pruebas automáticas (local)

- PARCIAL — `make test` (corrida 1, sin driver ODBC): isort y black sin cambios (39 archivos), flake8 limpio, pytest `5 failed, 189 passed`. Los 5 fallos son los de `test_synapse_integration.py` por `Can't open lib 'ODBC Driver 18 for SQL Server'`. Se repite cuando esté el driver.
  - HALLAZGO del checklist: dice que en `make test` las pruebas de Synapse se saltan, pero el skip solo aplica si no hay `SYNAPSE_WORKSPACE`. Con `.env.synapse` lleno corren contra el workspace real. Opciones: `addopts = -m "not synapse"` en `setup.cfg`, o corregir el texto del checklist.

## 2. Notebooks

Ejecutados headless con `jupyter nbconvert --execute` desde `docs/`, salida en `.superpowers/sdd/CHECKLIST_PRUEBAS/` (no versionada). Las salidas se leen con credenciales redactadas.

### 01_datalake.ipynb

Corrida 1 (FALLA):
- PASA — Configuración: `carpeta de trabajo: pruebas/cdf/cdf_notebook_5ebc2e13`.
- FALLA — 1. Cliente: `ValueError: unable to connect to account for Connection string is either blank or malformed`. Causa: `.env` tiene `AZURE_STORAGE_CONNECTION_STRING=` vacía (como en `datalake.env.example`); el notebook la carga con `override=True` y adlfs trata la cadena vacía como una conexión real en vez de usar `STORAGE_ACCOUNT_NAME` + identidad. Arrastra 2, 3 (resumir_ventas), 5 (run_task) y Limpieza.
  - HALLAZGO: el ejemplo debería dejar esa línea comentada, o el notebook/framework ignorar variables vacías (`get_datalake` ya lo hace; adlfs no).
- PASA — 3. `RecursoNoDeclarado`: imprime "La tarea 'usa_synapse_sin_declarar' usa 'synapse' sin declararlo…".
- PASA — 4. Conexión con nombre: imprime `datalake:curado: Configure AZURE_STORAGE_CONNECTION_STRING__CURADO o STORAGE_ACCOUNT_NAME__CURADO.`
- NOTA — 5. `check_resources()` devolvió `{'publicar_curado': [...]}` y no `{}`: es lo esperable cuando `datalake:curado` no está configurado (ver §4). El checklist es inconsistente en ese punto.
- Pendiente repetir cuando se corrija `.env`.

### Bloque 1, corrida 2 (con driver ODBC 18)

Entorno final: great-expectations 1.24.0, sqlalchemy 2.0.55, pyodbc 5.3.0 (las versiones de `poetry.lock`).

- PASA — isort y black sin cambios, flake8 limpio.
- FALLA — `make test`: `3 failed, 191 passed`. Dos fallos (`test_select_1`, `test_parameters_supported_on_serverless`) por `42109 The SQL pool is warming up. Please try again`: arranque en frío del serverless. Pasaron al repetir la suite.
  - HALLAZGO (código): `SynapseClient` no trata 42109 como transitorio (`TRANSIENT_CODES` en `synapse.py`), así que la primera consulta tras un rato inactivo falla sin reintento. En la Function esto se ve como una tarea fallida en el primer intento del día.
- `pytest -m synapse -v -s`: `1 failed, 4 passed`.
  - PASA — `test_select_1`, `test_parameters_supported_on_serverless`, `test_read_test_table`, `test_for_database`.
  - FALLA — `test_gx_native_sql_matches_pandas`: modo sql `success=False`, fallidas `expect_column_to_exist` y `expect_column_values_to_not_be_null`. El modo pandas sí pasa.
  - PASA — el SQL generado aparece bajo `--- SQL generado por GX ---`; solo lleva `SELECT count(*) ... FROM (<consulta con WHERE fecha = ...>) AS anon_1`, sin `SELECT *` ni `#`.
  - MB procesados anotados: sql=10.0, pandas=10.0 (granularidad mínima del DMV).
  - CAUSA (reproducida con `.superpowers/sdd/CHECKLIST_PRUEBAS/repro_gx_sql.py`): GX calcula `table.columns` con `get_sqlalchemy_column_metadata`; para un query asset el selectable es un `TextClause`, la introspección devuelve vacío y el `column_reflection_fallback` de SQL Server hace `selectable.name` → `AttributeError: 'TextClause' object has no attribute 'name'` → la métrica recibe `None` → `'NoneType' object is not iterable`. Ocurre igual con GX 1.24.1 + SQLAlchemy 2.1.6 y con GX 1.24.0 + SQLAlchemy 2.0.55. Solo las expectativas de tabla (conteo de filas) funcionan en modo sql contra Synapse. Las pruebas unitarias no lo detectan porque usan SQLite, cuyo fallback es distinto.

Corrida 2 (tras comentar `AZURE_STORAGE_CONNECTION_STRING` en `.env`): PASA.
- PASA — Configuración: `carpeta de trabajo: pruebas/cdf/cdf_notebook_1bfa1c41`.
- PASA — 1. Cliente: imprime el nombre de la cuenta (la cuenta configurada), autenticado con identidad (AzureCliCredential).
- PASA — 2. Escribir y leer: el CSV (3 filas) y el Parquet (solo `canal`, `valor`) se leen de vuelta; la lista de dos archivos diarios se lee como un solo DataFrame de 3 filas; link SAS: `SALTADO: el link SAS requiere AZURE_STORAGE_CONNECTION_STRING con AccountKey` (esperado sin AccountKey).
- PASA — 3. Tareas: `resumir_ventas` devuelve `2`; la tarea sin declarar imprime el error `RecursoNoDeclarado`.
- PASA — 4. Conexión con nombre: imprime lo que falta para `datalake:curado`.
- PASA (con nota) — 5. Runner: `check_resources()` imprime `{'publicar_curado': [...datalake:curado...]}` (no `{}`, porque `datalake:curado` no está configurado; ver nota de la corrida 1); `run_task('resumir_ventas')` termina sin error y devuelve `2`.
- PASA — Limpieza: imprime `borrada: pruebas/cdf/cdf_notebook_1bfa1c41`; `az storage blob list` con ese prefijo devuelve 0 blobs.

### 02_great_expectations.ipynb

- PASA — Configuración: `storage: sí`, `datalake (sección 4): pruebas/cdf`, `synapse (sección 5): sí`.
- PASA — 1. En memoria: `success: True | fallidas: []` y la URL del reporte (local, `file:///tmp/...`; ver nota).
- PASA — 2. Fallo de calidad: directa `success: False | fallidas: ['expect_column_values_to_be_between']`; por el runner `error de calidad: ['expect_column_values_to_be_between']`.
- PASA — 3. Silencio: no lanza error (`success: False` sin excepción, con el WARNING de DQ silenciada) y muestra el aviso `el silencio de DQ vence el 2099-12-31`.
- PASA — 4. Datalake: `datalake -> success: True | fallidas: []`; `az storage blob list` con prefijo `cdf/cdf_gx_notebook_` devuelve 0 blobs.
- FALLA — 5. Synapse:
  - `sql -> success: False | fallidas: ['expect_column_values_to_not_be_null', 'expect_column_values_to_be_in_set', 'expect_column_values_to_be_between'] | MB procesados: 0.0`; `pandas -> success: True | fallidas: [] | MB procesados: 0.0`. No dan el mismo resultado. Misma causa que el bloque 1 (`table.columns` sobre TextClause en SQL Server).
  - El caso con `-5.0` da `sql -> success: False` pero con las tres expectativas de columna fallidas, no solo `between`: el fallo no demuestra que la expectativa se evaluó en Synapse, sino que ninguna de columna se evalúa.
  - La tabla real da `success: False | fallidas: ['expect_column_values_to_not_be_null'] | MB procesados: 10.0`.
  - NOTA — con solo `STORAGE_ACCOUNT_NAME` (identidad) los reportes de GX quedan locales (`file:///tmp/...`): `get_context` solo configura stores Azure con connection string. El notebook dice `storage: sí` igual. Es coherente con el código, pero el texto puede confundir.

### 03_synapse.ipynb

- PASA — Configuración: sin errores; `SYNAPSE_ODBC_DRIVER` está en `pyodbc.drivers()`.
- PASA — 1. Cliente: `engine_edition: 11`, usuario de Felipe, base `bronze`.
- PASA — 2. Leer: la consulta con parámetros devuelve `[{'texto': 'a', 'numero': 7}]`; `render` rechaza `'x; DROP VIEW v'`; el conteo y las columnas de `SYNAPSE_TEST_TABLE`.
- PASA — 3. Tareas: `contar_tabla` devuelve el mismo conteo.
- SALTADO — 4. Transformar: `SYNAPSE_TEST_DDL_SCHEMA` vacío (prueba de solo lectura por decisión de Felipe).
- PASA — 5. Varias bases: `('bronze', 'silver')`.
- FALLA — 6. Calidad: `sql: success=False failed=['expect_column_values_to_not_be_null'] MB procesados=10.0`; `pandas: success=True failed=[] MB procesados=10.0`. Misma causa que el bloque 1.
- PASA — 7. Runner: `recursos configurados` y el conteo.
- PASA — 8. pandas/polars: muestra 10 filas con pandas; `polars no está instalado (opcional)`.

### Poda de particiones (con la vista real)

- INCONCLUSO — `SELECT COUNT_BIG(*)` sobre la consulta de `SYNAPSE_TEST_GX_QUERY` con y sin el `WHERE fecha = '<foto>'`, dos rondas, sin otras consultas en paralelo (script `.superpowers/sdd/CHECKLIST_PRUEBAS/poda_particiones.py`):

  | consulta | filas | MB procesados |
  |---|---|---|
  | con filtro | N | 10,0 |
  | sin filtro | N | 10,0 |

  La vista tiene una sola foto (`COUNT(DISTINCT fecha) = 1`, 2026-09-24), así que el filtro no descarta nada y los MB tienen que salir iguales. No se puede concluir si Synapse poda carpetas. Además `sys.dm_external_data_processed` avanza de a 10 MB, mínimo por consulta. Para esta prueba hace falta una vista con dos o más fotos y una lectura claramente mayor a 10 MB.

## Resumen de la corrida

| Bloque | Resultado |
|---|---|
| 0. Preparación | Completo |
| 1. Pruebas automáticas | FALLA: `test_gx_native_sql_matches_pandas` (GX modo sql) y arranque en frío 42109 sin reintento |
| 2. Notebooks 01 y 02 §1-4 | PASA |
| 2. Notebook 02 §5 y 03 §6 | FALLA por la misma causa de GX modo sql |
| 2. Notebook 03 §4 | SALTADO (sin esquema DDL) |
| 2. Poda de particiones | INCONCLUSO (vista con una sola foto) |
| 3, 4, 5 | Fuera de alcance de esta corrida |

Según la regla del checklist ("si un bloque falla, no siga"), no se puede pasar al bloque 3 hasta resolver el modo sql de GX.

## Prueba de hipótesis: table asset en modo sql

Hipótesis: el defecto del modo sql aparece solo cuando GX debe averiguar las columnas de una consulta suelta (`add_query_asset`); con una tabla o vista con nombre (`add_table_asset` + `add_batch_definition_whole_table`) GX las obtiene del catálogo y el modo sql funciona.

Prueba (diagnóstico, sin tocar la librería; script y salidas en `.superpowers/sdd/`, ignorado por git): misma fuente SQL de la librería, table asset sobre la vista de `SYNAPSE_TEST_TABLE` con esquema y nombre separados, suite de la prueba de integración (conteo de filas, `expect_column_to_exist` y `expect_column_values_to_not_be_null` sobre la columna `id`) más un caso que debe fallar (`expect_column_values_to_be_between` con `max_value=-1`). Misma suite en modo pandas para comparar. Entorno: versiones del `poetry.lock`.

Resultado: **la hipótesis se confirma.**

| Expectativa | sql (table asset) | pandas |
|---|---|---|
| expect_table_row_count_to_be_between | PASA | PASA |
| expect_column_to_exist | PASA | PASA |
| expect_column_values_to_not_be_null | PASA, 0 nulos | PASA, 0 nulos |
| expect_column_values_to_be_between (forzada) | FALLA, 100 % fuera de rango | FALLA, 100 % fuera de rango |

- Las expectativas de columna pasan en modo sql y el caso forzado falla con el conteo completo de filas fuera de rango: la expectativa se evaluó en Synapse, no en vacío.
- Los dos modos dan exactamente las mismas fallidas.
- Causa: con table asset GX resuelve `table.columns` con el inspector de SQLAlchemy (consulta a `sys.columns` por el nombre del objeto) y nunca entra al `column_reflection_fallback` que rompe con la consulta suelta.
- SQL generado: solo agregados y catálogo. Sin tablas temporales (`#`). Observaciones:
  - GX envuelve la fuente como `SELECT * FROM <esquema>.<vista> WHERE 1 = 1` en una subconsulta; por encima proyecta solo las columnas que usa (`count(*)`, `sum(CASE WHEN ...)`). El `SELECT *` literal está, aunque el motor puede podar columnas al optimizar; conviene medir MB con una vista ancha antes de darlo por inocuo.
  - Con `result_format` SUMMARY, GX además ejecuta un `SELECT TOP 20 <columna> AS unexpected_values ...` por expectativa de columna para traer ejemplos. Son lecturas extra sobre la misma vista.
  - Un table asset "whole table" no lleva el filtro de partición (`WHERE fecha = ...`). Para podar carpetas habría que usar un batch definition con partición de GX sobre la columna de fecha, o seguir validando por consulta cuando se necesite el filtro.

Implicación para el brainstorming: agregar a la librería la validación por tabla o vista (caso principal) y decidir cómo tratar los cruces, por ejemplo publicándolos como vista con nombre; y revisar cómo aplicar el filtro de partición en el table asset.
