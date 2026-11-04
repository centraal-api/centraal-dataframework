# Checklist de pruebas antes de publicar

Va en orden: local, notebooks, imagen de Functions, la librería en una Function de Azure y publicación.
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
- [ ] Construir la wheel de prueba, con la misma versión en los dos lugares donde vive (`/api/diag` lee `__version__`, no `pyproject.toml`):
  - [ ] `poetry version 0.3.0.dev0`;
  - [ ] `sed -i "s/^__version__ = '.*'/__version__ = '0.3.0.dev0'/" centraal_dataframework/__init__.py`;
  - [ ] `poetry build`, y comprobar que `unzip -p dist/*.whl centraal_dataframework/__init__.py | grep __version__` dice `0.3.0.dev0`;
  - [ ] `git checkout -- pyproject.toml centraal_dataframework/__init__.py`, para volver a la versión del repo; `git status` queda limpio (`dist/` está en `.gitignore`).

## 4. La librería en una Function de Azure

Dos formas de desplegar, las dos válidas: **A** código en plan Premium (Elastic Premium, Linux, `infra/main.bicep` de
`init`) o **B** contenedor en plan Dedicado (App Service Linux, imagen propia sobre la oficial de Functions). Evidencia:
corrida 5 del registro (`docs/superpowers/runs/2026-11-02-checklist-pruebas.md`), hecha con la forma B en un plan B1.

### Infraestructura

- [x] Plan e instancia. **A:** `infra/main.bicep` con `instancias=1`, `pythonVersion=3.12` y `synapseWorkspace=<ws>`; en el plan, *Scale out* con mínimo 1, máximo 1 y ninguna pre-calentada (`az functionapp show ... --query siteConfig.preWarmedInstanceCount`). **B:** plan App Service Linux (B1 o mayor) con `capacity: 1`, `alwaysOn: true` (sin Always On el host se duerme y los timers paran), `linuxFxVersion: DOCKER|<imagen con digest>`, `WEBSITES_ENABLE_APP_SERVICE_STORAGE=false` y pull del registro con identidad. — corrida 5 (B).
- [x] Storage del host por identidad, sin llave: `AzureWebJobsStorage__accountName` (con identidad asignada por el usuario, además `__credential=managedidentity`, `__clientId` y `AZURE_CLIENT_ID`); roles *Storage Blob Data Owner*, *Storage Queue Data Contributor* y *Storage Table Data Contributor*; cuenta sin namespace jerárquico. Cubre la cola, el timer y el historial. — corrida 5.
- [x] Identidad de la app en Synapse serverless, sin privilegios de administrador: `CREATE USER [<identidad>] FROM EXTERNAL PROVIDER`, `db_datareader`, `GRANT VIEW DEFINITION` (lo exige `validate_table`) y, si las vistas leen el lago con `OPENROWSET` sobre una fuente con credencial, `GRANT ADMINISTER DATABASE BULK OPERATIONS` y `GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[<credencial>]`. Sin estos dos, error 4834 *bulk load statement*; un administrador no lo nota porque ya los tiene. — corrida 5.
- [x] Red. **A:** las IP de salida de la Function en el firewall del workspace. **B con VNet:** subred propia delegada a `Microsoft.Web/serverFarms`, *Route All* y NAT Gateway; la IP del NAT es la única regla de la Function en el firewall. — corrida 5 (B con VNet).
- [x] `YAML_CONFIGURACION` con **ruta absoluta** (`/home/site/wwwroot/centraal_dataframework.yaml`): el worker no arranca en la carpeta de la app. — corrida 5.

### Despliegue

- [ ] **A:** la wheel está en `wheels/` y `requirements.txt` la referencia con `[synapse]`; `func azure functionapp publish <app>` termina y el log del build remoto muestra que instaló la wheel.
- [x] **B:** `requirements.txt` con `centraal-dataframework[synapse] @ https://github.com/centraal-api/centraal-dataframework/archive/<sha>.tar.gz` (commit fijado por SHA, ya empujado); el Dockerfile comprueba `'ODBC Driver 18 for SQL Server' in pyodbc.drivers()` y la construcción falla si la imagen base dejó de traerlo; las pruebas de la app corren dentro de la imagen en una etapa `pruebas` (`docker build --target pruebas`), antes de la etapa `app`, así que una imagen con pruebas rotas no existe; imagen construida con `az acr build` y referenciada por digest. — corrida 5.

### Verificación

- [x] `GET /api/diag?code=<key>` responde 200 con `version` (`0.3.0.dev0` con la wheel de prueba; la del commit, por ejemplo `0.2.0`, con el tarball), `python` 3.12.x, el driver 18 en `odbc_drivers` y `problemas: {}`. Si el host arranca con cero funciones y Application Insights solo dice `Worker failed to index functions`, reproduzca en local con la misma imagen (`PRUEBA_EN_FUNCTION.md`). — corrida 5.
- [x] `POST /api/queue_task?code=<key>` con `{"task_name": "<tarea>", "params": {...}}`: en Application Insights aparecen el inicio, el fin y `tarea_capacidad` con `espera_cola_min`; con `HISTORY_ENABLED=true`, `GET /api/status/<run_id>` muestra `success` y `GET /api/status?task=<tarea>` lista la corrida. — corrida 5.
- [x] Una tarea con `validate_table(..., particion={...}, modo='sql')` termina bien y el log muestra `GX <nombre> [sql] tabla=... particion=... success=True data_processed_mb=...`. — corrida 5 (40 MB: cuatro consultas al mínimo de 10 MB).
- [x] Un fallo de calidad forzado notifica en el primer intento y no se reintenta: una sola ejecución de `execute_tasks_inqueue`, sin cola `*-poison`, historial `failed`. — corrida 5.
- [x] Una partición sin datos falla por calidad (`expect_table_row_count_to_be_between`), no por motor, y no se reintenta. — corrida 5.
- [x] Parámetros inválidos (fecha que no es ISO, identificador con `;`) fallan con `ValueError` antes de abrir Synapse (cero trazas `GX`). Hoy la cola los reintenta hasta `maxDequeueCount` como cualquier error de tarea; si debe reintentar o no es una decisión aparte (ver `PRUEBA_EN_FUNCTION.md`). — corrida 5.
- [x] En serie: encolar `{"task_name": ["a", "b", "c"]}`. Las tareas corren una tras otra y en orden (`requests` sin solapar); `espera_cola_min` solo crece si las tareas duran más que unos segundos. — corrida 5.
- [ ] El timer `monitor_capacidad` deja `capacidad {...}` cada 30 min, con `memoria_limite_mb` cerca de 1750 en B1 y de 3500 en EP1. Corrida 5: las trazas llegaron, pero con `memoria_limite_mb: null` en el contenedor de App Service (pendiente de arreglo).

### Alertas y costos

- [x] Prueba de disponibilidad de Application Insights sobre `/api/diag`, con alerta si falla. — corrida 5 (tres ubicaciones).
- [x] Alerta sobre las trazas que contienen `CAPACIDAD:` (regla de consulta programada sobre `traces | where message startswith "CAPACIDAD:"`, cada 30 min, umbral mayor que 0, severidad 3, hacia el action group del ambiente). — corrida 5 (desplegada; sin disparo observado porque no hubo advertencias).
- [ ] `sp_set_data_processed_limit` configurado en el workspace.
- [ ] Un día de operación normal sin advertencias `CAPACIDAD:`, o con advertencias explicadas.

## 5. Publicar

- [ ] Todo lo anterior marcado.
- [ ] `bump2version` a la versión elegida, y mover `[Unreleased]` en el CHANGELOG.
- [ ] Merge a main y push del tag `v<versión>`; `release.yml` publica en PyPI.
- [ ] En la Function: cambiar la wheel (A) o el tarball del commit (B) por `centraal-dataframework[synapse]==<versión>`, borrar `wheels/` si existe, redesplegar o reconstruir la imagen, y repetir `/api/diag`.
