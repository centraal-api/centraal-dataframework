# Checklist de pruebas antes de publicar

Va en orden: local, notebooks, imagen de Functions y build, publicación en PyPI y verificación en una Function de
Azure con la forma soportada ([DESPLIEGUE_FUNCTION.md](DESPLIEGUE_FUNCTION.md)). Si un bloque falla, no siga al
siguiente. Más detalle en [CAPACIDAD.md](CAPACIDAD.md).

Cada ítem marcado cita la corrida que lo prueba; la evidencia está en
[docs/superpowers/runs/2026-11-02-checklist-pruebas.md](superpowers/runs/2026-11-02-checklist-pruebas.md).

## Estado al 2026-11-04

| Bloque | Estado | Corrida |
|---|---|---|
| 0. Preparación | Completo | 1 y 2 |
| 1. Pruebas automáticas | Completo | 3 |
| 2. Notebooks y poda | Completo (03 §4 saltado: opcional, sin esquema DDL) | 3 |
| 3. Imagen de Functions y build | Completo | 4 y ensayo de release |
| 4. Publicar 0.3.0 en PyPI | En curso | — |
| 5. Verificación en la Function (forma soportada) | Pendiente: se ejecuta en el repo del cliente con [el plan](superpowers/plans/2026-11-04-despliegue-function-cliente.md) | — |

La forma en contenedor (plan Dedicado) se verificó en la corrida 5 sobre `f317ddc`. Ya no está soportada: queda como
referencia en [DESPLIEGUE_CONTENEDOR.md](DESPLIEGUE_CONTENEDOR.md).

## 0. Preparación (una vez por máquina)

- [x] Instalar la librería con el extra: `pip install -e ".[synapse]"`, más `jupyter` y `python-dotenv`. — corrida 1.
- [x] Driver ODBC: `python -c "import pyodbc; print(pyodbc.drivers())"` incluye `ODBC Driver 18 for SQL Server`. — corrida 2.
- [x] `az login` (con `--tenant` si aplica), con un usuario que tenga acceso al lago y a Synapse. — corrida 1.
- [x] `.env` en la raíz, copiado de [datalake.env.example](datalake.env.example), con: — corrida 2.
  - `AZURE_STORAGE_CONNECTION_STRING` (con AccountKey) o `STORAGE_ACCOUNT_NAME`;
  - `DATALAKE_TEST_PATH=contenedor/carpeta`, donde se pueda escribir y borrar.
- [x] `.env.synapse` en la raíz, copiado de [synapse.env.example](synapse.env.example), con: — corridas 1 y 3.
  - `SYNAPSE_WORKSPACE` y `SYNAPSE_DATABASE`;
  - `SYNAPSE_TEST_TABLE`: una vista de bronze;
  - `SYNAPSE_TEST_GX_QUERY`: un SELECT de esa vista con 2 o 3 columnas y `WHERE fecha = '<foto>'`;
  - `SYNAPSE_TEST_GX_COLUMN`: una columna de esa consulta;
  - `SYNAPSE_TEST_PARTITION_TABLE`, `SYNAPSE_TEST_PARTITION_COLUMN`, `SYNAPSE_TEST_PARTITION_VALUE`: la vista de
    prueba con la trampa (dos fotos sanas y una carpeta con un `.parquet` dañado, en el contenedor de pruebas, con su
    propio `OPENROWSET ... WITH (...)` en un esquema de prueba), su columna de partición y una foto sana;
  - opcionales: `SYNAPSE_TEST_OTHER_DB` y `SYNAPSE_TEST_DDL_SCHEMA`.
- [x] Su IP en el firewall del workspace y su usuario en cada base: `CREATE USER [...] FROM EXTERNAL PROVIDER` + `db_datareader` + `GRANT VIEW DEFINITION`. — corrida 1 (hecho por un admin de Entra).

## 1. Pruebas automáticas (local)

- [x] `make test`: isort, black, flake8 y pytest en verde. Con `.env.synapse` lleno, las pruebas de Synapse también corren aquí; sin él se saltan. — corrida 3 (`250 passed, 1 skipped`).
- [x] `pytest -m synapse -v -s` contra el workspace real: — corrida 3 (`9 passed`).
  - [x] pasan `test_select_1`, `test_parameters_supported_on_serverless`, `test_read_test_table` y `test_for_database`;
  - [x] pasa `test_gx_query_sql_with_columns_is_rejected`: `validate_query` en sql con columnas da `ValueError`, y en pandas pasa;
  - [x] pasa `test_validate_table_particion_sql_matches_pandas`, y en la salida aparece `--- SQL generado por GX ---`;
  - [x] ese SQL lleva `WHERE <columna> = ?`, sin `DISTINCT`, sin `TOP 1 1`, sin funciones sobre la columna ni tablas temporales (`#`);
  - [x] pasan `test_validate_table_completa_falla_por_trampa` (error de Parquet como `ErrorMotorCalidad`), `test_validate_table_sin_particion_se_niega` y `test_validate_table_columna_que_no_es_particion`;
  - [x] anotar los `MB procesados` de `sql` y de `pandas` (cada consulta cuenta 10 MB como mínimo). — sql 60 MB (seis consultas), pandas 25 MB.

## 2. Notebooks (todas las celdas, de arriba abajo, reiniciando el kernel antes de cada uno)

### [01_datalake.ipynb](01_datalake.ipynb) (requiere `.env`) — corridas 2 y 3

- [x] Configuración: imprime `carpeta de trabajo: .../cdf_notebook_xxxx`.
- [x] 1. Cliente: imprime el nombre de la cuenta.
- [x] 2. Escribir y leer:
  - el CSV y el Parquet se leen de vuelta;
  - la lista de archivos se lee como un solo DataFrame;
  - se genera el link SAS (o dice `SALTADO` si no hay AccountKey).
- [x] 3. Tareas:
  - `resumir_ventas` devuelve `2`;
  - la tarea sin declarar el recurso imprime el error `RecursoNoDeclarado`.
- [x] 4. Conexión con nombre: imprime lo que falta configurar para `datalake:curado`, o `configuración completa`.
- [x] 5. Runner: `check_resources()` imprime `{}`, o solo la tarea que usa `datalake:curado` si esa conexión no está configurada (como en el punto 4), y `run_task` termina sin error.
- [x] Limpieza: imprime `borrada:` y la carpeta ya no está en el lago.

### [02_great_expectations.ipynb](02_great_expectations.ipynb) (sin variables corre de la sección 1 a la 3) — corrida 3

- [x] Configuración: dice si hay storage, datalake y Synapse.
- [x] 1. En memoria: `success: True | fallidas: []`, más la URL del reporte.
- [x] 2. Fallo de calidad:
  - la llamada directa da `success: False | fallidas: ['expect_column_values_to_be_between']`;
  - por el runner sale `error de calidad: [...]`.
- [x] 3. Silencio: no lanza error y muestra el aviso de vencimiento.
- [x] 4. Datalake: `datalake -> success: True`, y la carpeta temporal se borra.
- [x] 5. Synapse (requiere `SYNAPSE_TEST_PARTITION_*`):
  - [x] `validate_table` de la foto sana: `sql -> success: True` y `pandas -> success: True`, con el mismo resultado y los MB procesados;
  - [x] el caso forzado da `sql -> success: False | fallidas: ['expect_column_values_to_be_null']`, o sea que la expectativa se evaluó en Synapse;
  - [x] sin `particion` imprime el `ParticionInvalida` que pide declararla o `'completa'`;
  - [x] `validate_query` del cruce en línea: `pandas -> success: True`, y en `sql` imprime el `ValueError` que remite a `validate_table`.

### [03_synapse.ipynb](03_synapse.ipynb) (requiere `.env.synapse`) — corrida 3

- [x] Configuración: el driver configurado aparece en `pyodbc.drivers()`.
- [x] 1. Cliente: `engine_edition = 11` (serverless), con el usuario y la base esperados.
- [x] 2. Leer:
  - la consulta con parámetros devuelve una fila;
  - `render` rechaza el valor inseguro;
  - salen el conteo y las columnas de `SYNAPSE_TEST_TABLE`.
- [x] 3. Tareas: `contar_tabla` devuelve el mismo conteo.
- [ ] 4. Transformar (con `SYNAPSE_TEST_DDL_SCHEMA`): crea la vista, la cuenta y la borra. — SALTADO en las corridas 2 y 3: opcional, la prueba fue de solo lectura. No bloquea la publicación.
- [x] 5. Varias bases (con `SYNAPSE_TEST_OTHER_DB`): devuelve los nombres de las dos bases.
- [x] 6. Calidad: `validate_table` con partición en `sql` y `pandas` da el mismo resultado, se imprimen los MB procesados, y el cruce por `validate_query` en pandas pasa.
- [x] 7. Runner: imprime `recursos configurados` y el conteo.
- [x] 8. pandas/polars: muestra 10 filas.

### Poda de particiones (con la trampa) — corrida 3

Los MB no sirven para medirla: cada consulta cobra 10 MB como mínimo y serverless lee solo las columnas que usa. Se usa la vista de `SYNAPSE_TEST_PARTITION_TABLE`, que tiene dos fotos sanas y una carpeta con un `.parquet` dañado.

- [x] `validate_table` de la foto sana en `sql` pasa: ni el registro del asset ni la suite tocaron la carpeta dañada.
- [x] `particion='completa'` falla con `ErrorMotorCalidad` y el error de Parquet: la trampa funciona.

Si la foto sana falla con el error de Parquet, Synapse no está podando: revise que la columna salga de `filepath(n)` con el nombre de la carpeta, y el SQL generado, antes de seguir.

## 3. Imagen de Functions y build (local, con Docker)

- [x] `docker run --rm mcr.microsoft.com/azure-functions/python:4-python3.12 odbcinst -q -d` lista `[ODBC Driver 18 for SQL Server]`. — corrida 4 (msodbcsql18 18.7.1.1).
- [x] Ensayo del workflow de release en un entorno limpio, con los mismos pasos de `release.yml`: — ensayo de release (2026-11-05, Python 3.12 en Windows, sin Docker).
  - [x] `pip install poetry`, `poetry install --with doc` y `poetry run mkdocs build` terminan sin error. El ensayo encontró que `mkdocs build` fallaba con las versiones de `poetry.lock` (`mkdocs-autorefs` 1.4 con `mkdocstrings` 0.24); arreglado declarando `autorefs` en `mkdocs.yml`. En Windows con 3.12, `regex` 2022.10.31 del lock no trae wheel y pide compilador; el workflow usa Linux con 3.11, donde sí la trae;
  - [x] `poetry build` deja la wheel y el sdist con la versión a publicar;
  - [x] la wheel se instala en limpio con el extra (`pip install "<wheel>[synapse]"`) en Python 3.12, `import centraal_dataframework.blueprints` funciona y `centraal-dataframework init` genera `requirements.txt` con `==<versión>`.

## 4. Publicar

- [x] Bloques 0 a 3 marcados.
- [x] Versión en `pyproject.toml`, `centraal_dataframework/__init__.py` y `.bumpversion.cfg`, y `[Unreleased]` movido en el CHANGELOG.
- [ ] Merge a main y push del tag `v<versión>`; `release.yml` publica en PyPI.
- [ ] La versión aparece en PyPI y se instala: `pip install "centraal-dataframework[synapse]==<versión>"`.

## 5. Verificación en una Function de Azure (forma soportada)

Despliegue de código en plan Premium con la versión publicada, generado con `init`
([DESPLIEGUE_FUNCTION.md](DESPLIEGUE_FUNCTION.md)). Se ejecuta con el
[plan del repo del cliente](superpowers/plans/2026-11-04-despliegue-function-cliente.md). Los ítems con *(B)* ya se
vieron en la forma en contenedor (corrida 5); en la forma soportada se repiten.

### Infraestructura

- [ ] `infra/main.bicep` con `instancias=1`, `pythonVersion=3.12` y `synapseWorkspace=<ws>`; en el plan, *Scale out* con mínimo 1, máximo 1 y ninguna pre-calentada (`az functionapp show ... --query siteConfig.preWarmedInstanceCount`).
- [ ] Application Insights conectado y `YAML_CONFIGURACION` con ruta absoluta, tal como los deja el bicep.
- [ ] Identidad de la app en Synapse serverless, sin privilegios de administrador: `CREATE USER [<identidad>] FROM EXTERNAL PROVIDER`, `db_datareader`, `GRANT VIEW DEFINITION` y, si las vistas leen el lago con `OPENROWSET` sobre una fuente con credencial, `GRANT ADMINISTER DATABASE BULK OPERATIONS` y `GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[<credencial>]`. *(B: corrida 5.)*
- [ ] Red: las IP de salida de la Function en el firewall del workspace, o VNet con NAT Gateway. *(B: corrida 5, con VNet.)*

### Despliegue

- [ ] `requirements.txt` con `centraal-dataframework[synapse]==<versión>`; `func azure functionapp publish <app>` termina y el log del build remoto muestra que instaló esa versión desde PyPI.

### Verificación

- [ ] `GET /api/diag?code=<key>` responde 200 con `version` `<versión>`, `python` 3.12.x, el driver 18 en `odbc_drivers` y `problemas: {}`. *(B: corrida 5.)*
- [ ] `POST /api/queue_task?code=<key>` con `{"task_name": "<tarea>", "params": {...}}`: en Application Insights aparecen el inicio, el fin y `tarea_capacidad` con `espera_cola_min`; con `HISTORY_ENABLED=true`, `GET /api/status/<run_id>` muestra `success` y `GET /api/status?task=<tarea>` lista la corrida. *(B: corrida 5.)*
- [ ] Una tarea con `validate_table(..., particion={...}, modo='sql')` termina bien y el log muestra `GX <nombre> [sql] tabla=... particion=... success=True data_processed_mb=...`. *(B: corrida 5, 40 MB.)*
- [ ] Un fallo de calidad forzado notifica en el primer intento y no se reintenta: una sola ejecución de `execute_tasks_inqueue`, sin cola `*-poison`, historial `failed`. *(B: corrida 5.)*
- [ ] Una partición sin datos falla por calidad (`expect_table_row_count_to_be_between`), no por motor, y no se reintenta. *(B: corrida 5.)*
- [ ] Parámetros inválidos lanzados como `ParametrosInvalidos`: una sola ejecución, aviso en el primer intento, historial `failed` con el motivo, sin reintentos ni cola `*-poison`. Un `ValueError` común se sigue reintentando. (Arreglo `1f6824a`, posterior a la corrida 5.)
- [ ] Una tarea que falla y luego termina bien con el mismo `run_id` deja la fila del historial en `success` sin el `error` del intento anterior. (Arreglo `9e337f0`, posterior a la corrida 5.)
- [ ] En serie: encolar `{"task_name": ["a", "b", "c"]}`. Las tareas corren una tras otra y en orden (`requests` sin solapar). *(B: corrida 5.)*
- [ ] El timer `monitor_capacidad` deja `capacidad {...}` cada 30 min, con `memoria_limite_mb` cerca de 3500 en EP1. **No bloqueante:** si sigue en `null`, queda como mejora futura. (En B salió `null`; arreglo `cce8a1b`.)

### Alertas y costos

- [ ] Prueba de disponibilidad de Application Insights sobre `/api/diag`, con alerta si falla. *(B: corrida 5.)*
- [ ] Alerta sobre las trazas que contienen `CAPACIDAD:` (regla de consulta programada sobre `traces | where message startswith "CAPACIDAD:"`, cada 30 min, umbral mayor que 0). *(B: corrida 5.)*
- [ ] `sp_set_data_processed_limit` configurado en el workspace.
- [ ] Un día de operación normal sin advertencias `CAPACIDAD:`, o con advertencias explicadas.
