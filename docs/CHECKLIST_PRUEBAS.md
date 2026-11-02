# Checklist de pruebas antes de publicar

Va en orden: local, notebooks, imagen de Functions, Function Premium en Azure y publicación.
Si un bloque falla, no siga al siguiente. Más detalle en [PRUEBA_EN_FUNCTION.md](PRUEBA_EN_FUNCTION.md)
y [CAPACIDAD.md](CAPACIDAD.md).

## 0. Preparación (una vez por máquina)

- [ ] Instalar la librería con el extra: `pip install -e ".[synapse]"`, más `jupyter` y `python-dotenv`.
- [ ] Driver ODBC: `python -c "import pyodbc; print(pyodbc.drivers())"` incluye `ODBC Driver 18 for SQL Server`.
- [ ] `az login` (con `--tenant` si aplica), con un usuario que tenga acceso al lago y a Synapse.
- [ ] `.env` en la raíz, copiado de [datalake.env.example](datalake.env.example), con:
  - `AZURE_STORAGE_CONNECTION_STRING` (con AccountKey) o `STORAGE_ACCOUNT_NAME`;
  - `DATALAKE_TEST_PATH=contenedor/carpeta`, donde se pueda escribir y borrar.
- [ ] `.env.synapse` en la raíz, copiado de [synapse.env.example](synapse.env.example), con:
  - `SYNAPSE_WORKSPACE` y `SYNAPSE_DATABASE`;
  - `SYNAPSE_TEST_TABLE`: una vista de bronze;
  - `SYNAPSE_TEST_GX_QUERY`: un SELECT de esa vista con 2 o 3 columnas y `WHERE fecha = '<foto>'`;
  - `SYNAPSE_TEST_GX_COLUMN`: una columna de esa consulta;
  - `SYNAPSE_TEST_PARTITION_TABLE`, `SYNAPSE_TEST_PARTITION_COLUMN`, `SYNAPSE_TEST_PARTITION_VALUE`: la vista de
    prueba con la trampa (dos fotos sanas y una carpeta con un `.parquet` dañado, en el contenedor de pruebas, con su
    propio `OPENROWSET ... WITH (...)` en un esquema de prueba), su columna de partición y una foto sana;
  - opcionales: `SYNAPSE_TEST_OTHER_DB` y `SYNAPSE_TEST_DDL_SCHEMA`.
- [ ] Su IP en el firewall del workspace y su usuario en cada base: `CREATE USER [...] FROM EXTERNAL PROVIDER` + `db_datareader` + `GRANT VIEW DEFINITION`.

## 1. Pruebas automáticas (local)

- [ ] `make test`: isort, black, flake8 y pytest en verde. Con `.env.synapse` lleno, las pruebas de Synapse también corren aquí; sin él se saltan.
- [ ] `pytest -m synapse -v -s` contra el workspace real:
  - [ ] pasan `test_select_1`, `test_parameters_supported_on_serverless`, `test_read_test_table` y `test_for_database`;
  - [ ] pasa `test_gx_query_sql_with_columns_is_rejected`: `validate_query` en sql con columnas da `ValueError`, y en pandas pasa;
  - [ ] pasa `test_validate_table_particion_sql_matches_pandas`, y en la salida aparece `--- SQL generado por GX ---`;
  - [ ] ese SQL lleva `WHERE <columna> = ?`, sin `DISTINCT`, sin `TOP 1 1`, sin funciones sobre la columna ni tablas temporales (`#`);
  - [ ] pasan `test_validate_table_completa_falla_por_trampa` (error de Parquet como `ErrorMotorCalidad`), `test_validate_table_sin_particion_se_niega` y `test_validate_table_columna_que_no_es_particion`;
  - [ ] anotar los `MB procesados` de `sql` y de `pandas` (cada consulta cuenta 10 MB como mínimo).

## 2. Notebooks (todas las celdas, de arriba abajo, reiniciando el kernel antes de cada uno)

### [01_datalake.ipynb](01_datalake.ipynb) (requiere `.env`)

- [ ] Configuración: imprime `carpeta de trabajo: .../cdf_notebook_xxxx`.
- [ ] 1. Cliente: imprime el nombre de la cuenta.
- [ ] 2. Escribir y leer:
  - el CSV y el Parquet se leen de vuelta;
  - la lista de archivos se lee como un solo DataFrame;
  - se genera el link SAS (o dice `SALTADO` si no hay AccountKey).
- [ ] 3. Tareas:
  - `resumir_ventas` devuelve `2`;
  - la tarea sin declarar el recurso imprime el error `RecursoNoDeclarado`.
- [ ] 4. Conexión con nombre: imprime lo que falta configurar para `datalake:curado`, o `configuración completa`.
- [ ] 5. Runner: `check_resources()` imprime `{}` y `run_task` termina sin error.
- [ ] Limpieza: imprime `borrada:` y la carpeta ya no está en el lago.

### [02_great_expectations.ipynb](02_great_expectations.ipynb) (sin variables corre de la sección 1 a la 3)

- [ ] Configuración: dice si hay storage, datalake y Synapse.
- [ ] 1. En memoria: `success: True | fallidas: []`, más la URL del reporte.
- [ ] 2. Fallo de calidad:
  - la llamada directa da `success: False | fallidas: ['expect_column_values_to_be_between']`;
  - por el runner sale `error de calidad: [...]`.
- [ ] 3. Silencio: no lanza error y muestra el aviso de vencimiento.
- [ ] 4. Datalake: `datalake -> success: True`, y la carpeta temporal se borra.
- [ ] 5. Synapse (requiere `SYNAPSE_TEST_PARTITION_*`):
  - [ ] `validate_table` de la foto sana: `sql -> success: True` y `pandas -> success: True`, con el mismo resultado y los MB procesados;
  - [ ] el caso forzado da `sql -> success: False | fallidas: ['expect_column_values_to_be_null']`, o sea que la expectativa se evaluó en Synapse;
  - [ ] sin `particion` imprime el `ParticionInvalida` que pide declararla o `'completa'`;
  - [ ] `validate_query` del cruce en línea: `pandas -> success: True`, y en `sql` imprime el `ValueError` que remite a `validate_table`.

### [03_synapse.ipynb](03_synapse.ipynb) (requiere `.env.synapse`)

- [ ] Configuración: el driver configurado aparece en `pyodbc.drivers()`.
- [ ] 1. Cliente: `engine_edition = 11` (serverless), con el usuario y la base esperados.
- [ ] 2. Leer:
  - la consulta con parámetros devuelve una fila;
  - `render` rechaza el valor inseguro;
  - salen el conteo y las columnas de `SYNAPSE_TEST_TABLE`.
- [ ] 3. Tareas: `contar_tabla` devuelve el mismo conteo.
- [ ] 4. Transformar (con `SYNAPSE_TEST_DDL_SCHEMA`): crea la vista, la cuenta y la borra.
- [ ] 5. Varias bases (con `SYNAPSE_TEST_OTHER_DB`): devuelve los nombres de las dos bases.
- [ ] 6. Calidad: `validate_table` con partición en `sql` y `pandas` da el mismo resultado, se imprimen los MB procesados, y el cruce por `validate_query` en pandas pasa.
- [ ] 7. Runner: imprime `recursos configurados` y el conteo.
- [ ] 8. pandas/polars: muestra 10 filas.

### Poda de particiones (con la trampa)

Los MB no sirven para medirla: cada consulta cobra 10 MB como mínimo y serverless lee solo las columnas que usa. Se usa la vista de `SYNAPSE_TEST_PARTITION_TABLE`, que tiene dos fotos sanas y una carpeta con un `.parquet` dañado.

- [ ] `validate_table` de la foto sana en `sql` pasa: ni el registro del asset ni la suite tocaron la carpeta dañada.
- [ ] `particion='completa'` falla con `ErrorMotorCalidad` y el error de Parquet: la trampa funciona.
- [ ] Si la foto sana falla con el error de Parquet, Synapse no está podando: revise que la columna salga de `filepath(n)` con el nombre de la carpeta, y el SQL generado, antes de seguir.

## 3. Imagen de Functions (local, con Docker Desktop encendido)

- [ ] `docker run --rm mcr.microsoft.com/azure-functions/python:4-python3.12 odbcinst -q -d` lista `[ODBC Driver 18 for SQL Server]`.
- [ ] Construir la wheel:
  - [ ] `poetry version 0.3.0.dev0 && poetry build`;
  - [ ] `git checkout -- pyproject.toml`, para volver a la versión del repo.

## 4. Function Premium en Azure

### Infraestructura

- [ ] Desplegar `infra/main.bicep` con `instancias=1`, `pythonVersion=3.12` y `synapseWorkspace=<ws>`.
- [ ] En el portal, en el plan, *Scale out*:
  - [ ] mínimo 1, máximo 1 y una sola instancia activa;
  - [ ] ninguna pre-calentada: `az functionapp show ... --query siteConfig.preWarmedInstanceCount`.
- [ ] Identidad de la app en Synapse: `CREATE USER [<app>] FROM EXTERNAL PROVIDER` + `db_datareader` en cada base, y `GRANT VIEW DEFINITION` en las que tengan vistas particionadas (lo exige `validate_table`; riesgo por verificar: las pruebas locales corren con un admin que ya lo tiene).
- [ ] Las IP de salida de la Function están en el firewall del workspace.
- [ ] Si la cola se lee con identidad: rol *Storage Queue Data Reader*.

### Despliegue

- [ ] La wheel está en `wheels/` y `requirements.txt` la referencia con `[synapse]`.
- [ ] `func azure functionapp publish <app>` termina, y el log del build remoto muestra que instaló la wheel.

### Verificación

- [ ] `GET /api/diag?code=<key>` responde 200 con:
  - `version` `0.3.0.dev0`;
  - `python` 3.12.x;
  - el driver 18 en `odbc_drivers`;
  - `problemas: {}`.
- [ ] `POST /api/queue_task?code=<key>` con `{"task_name": "<tarea>", "params": {...}}`:
  - [ ] en Application Insights aparecen el inicio, el fin y `tarea_capacidad` con `espera_cola_min`;
  - [ ] con `HISTORY_ENABLED=true`, `GET /api/status/<run_id>` muestra la ejecución exitosa.
- [ ] Una tarea con `validate_table(..., particion={...}, modo='sql')` termina bien, y el log muestra `GX <nombre> [sql] tabla=... particion=... success=True data_processed_mb=...`.
- [ ] Un fallo de calidad forzado notifica en el primer intento y no se reintenta.
- [ ] En serie: encolar `{"task_name": ["a", "b", "c"]}`. Cada tarea empieza cuando termina la anterior, y `espera_cola_min` crece.
- [ ] El timer `monitor_capacidad` deja `capacidad {...}` cada 30 min, con `memoria_limite_mb` cerca de 3500 en EP1.

### Alertas y costos

- [ ] Prueba de disponibilidad de Application Insights sobre `/api/diag`, con alerta si falla.
- [ ] Alerta sobre las trazas que contienen `CAPACIDAD:`.
- [ ] `sp_set_data_processed_limit` configurado en el workspace.
- [ ] Un día de operación normal sin advertencias `CAPACIDAD:`, o con advertencias explicadas.

## 5. Publicar

- [ ] Todo lo anterior marcado.
- [ ] `bump2version` a la versión elegida, y mover `[Unreleased]` en el CHANGELOG.
- [ ] Merge a main y push del tag `v<versión>`; `release.yml` publica en PyPI.
- [ ] En la Function: cambiar la wheel por `centraal-dataframework[synapse]==<versión>`, borrar `wheels/`, redesplegar y repetir `/api/diag`.
