# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `ctx.gx.validate_table(synapse, name, 'esquema.vista', expectations, particion={'columna': valor}, modo='sql'|'pandas')`: el caso principal de calidad en Synapse. Valida una tabla o vista con nombre; en bronze, solo la partición declarada. La partición se verifica contra la definición de la vista (`OBJECT_DEFINITION`: la columna debe salir de `filepath(n)` sobre la carpeta `<columna>=*`); una vista particionada sin `particion` se rechaza, y `particion='completa'` la valida entera a propósito. Excepción nueva `ParticionInvalida`.
- `ErrorMotorCalidad`: cuando GX no puede evaluar una expectativa (columna inexistente, archivo dañado) la validación lanza esta excepción en vez de reportar un falso fallo de calidad. Aplica a `validate_table` y `validate_query` en los dos modos.
- `TableAssetSynapse` (`gx_synapse.py`): table asset de GX que no sondea la tabla entera al registrarse ni lista los valores distintos de la columna de partición; aplica el filtro como `WHERE <columna> = ?`, sin funciones. `comprobar_api_gx()` es la prueba de humo de los nombres privados de GX de los que depende.
- `SynapseClient.tipo_objeto`, `SynapseClient.definicion_objeto` y `separar_tabla`; parser `particiones.py`.
- Variables de prueba `SYNAPSE_TEST_PARTITION_TABLE`, `SYNAPSE_TEST_PARTITION_COLUMN` y `SYNAPSE_TEST_PARTITION_VALUE` para la vista con la trampa de poda (dos fotos sanas y una carpeta con un Parquet dañado).

- Calidad de datos en Synapse sin traer los datos a la Function: `ctx.gx.validate_query(synapse, name, query, expectations, params=..., modo='sql')`. GX traduce cada expectativa a SQL y serverless devuelve solo agregados (plan A). Con `modo='pandas'` la misma suite valida la consulta traída con `read_df` (plan B). Acepta `suite_name`, usa `result_format='SUMMARY'` por defecto y, con `medir_costo=True`, deja `data_processed_mb` en el `DQResult`.
- `SynapseClient.sqlalchemy_url()`, `sqlalchemy_kwargs()` (token de Entra en `attrs_before`) y `data_processed_mb()`.
- Variable `SYNAPSE_ODBC_DRIVER` (por defecto `ODBC Driver 18 for SQL Server`). `check-resources` y el runner avisan si el driver configurado no está instalado.
- El extra `[synapse]` instala `sqlalchemy>=2.0`.
- Guía `docs/PRUEBA_EN_FUNCTION.md`: probar una versión sin publicar en una Azure Function.
- Endpoint `GET /api/diag`: versión, Python, drivers ODBC y problemas de configuración por tarea. Responde 503 si hay problemas, para usarlo en una prueba de disponibilidad.

- Monitor de capacidad: timer `monitor_capacidad` cada 30 min que mide la cola de tareas (mensajes y espera del más viejo) y la memoria pico del worker frente a la de la instancia. Deja la traza `capacidad` y, sobre los umbrales `CAPACIDAD_COLA_MAX`, `CAPACIDAD_ESPERA_MAX_MIN` y `CAPACIDAD_MEMORIA_PCT`, advertencias `CAPACIDAD:` que recomiendan otra instancia o una más grande. Cada ejecución de la cola deja `tarea_capacidad` con su espera y la memoria pico. Guía en `docs/CAPACIDAD.md`.
- Dependencia `azure-storage-queue` (para medir la cola).
- El historial (`HISTORY_ENABLED`) funciona con la conexión por identidad administrada del host: sin cadena de conexión, toma la cuenta de `<conexión>__accountName` (por ejemplo `AzureWebJobsStorage__accountName`) o de `STORAGE_ACCOUNT_NAME` y usa `DefaultAzureCredential`. Nuevo `Settings.resolve_storage_account_name()`.
- `docs/02_great_expectations.ipynb` muestra la misma suite sobre un DataFrame en memoria, un archivo del datalake y Synapse, en `modo='sql'` y `modo='pandas'`. Checklist de pruebas en `docs/CHECKLIST_PRUEBAS.md`.

### Changed

- `validate_query` en `modo='sql'` solo admite expectativas de tabla: con expectativas de columna lanza `ValueError` que remite a `validate_table` o a `modo='pandas'`. GX no resuelve las columnas de una consulta suelta en SQL Server (`column_reflection_fallback` sobre un `TextClause`), lo que producía falsos fallos de calidad.
- `great-expectations` acotado a `>=1.24,<1.25`: `TableAssetSynapse` depende de nombres privados de GX; subir la cota es una decisión explícita que pasa por `tests/test_gx_synapse.py` y `pytest -m synapse`.
- La identidad que valida necesita `GRANT VIEW DEFINITION` en las bases con vistas particionadas.
- La plantilla de `init` despliega en plan Premium (Elastic Premium, Linux) con Python 3.11/3.12, identidad administrada, build remoto y variables `SYNAPSE_*`. Usa una instancia siempre encendida, sin escalado elástico ni pre-calentada (`instancias=1`, que es mínimo y máximo a la vez). En `host.json`: `functionTimeout` de 1 h y la cola de a un mensaje por instancia (`batchSize=1`, `newBatchThreshold=0`). Antes la plantilla usaba Consumption (Y1).

### Fixed

- `docs/datalake.env.example` dejaba `AZURE_STORAGE_CONNECTION_STRING=` vacía y descomentada, lo que rompía la autenticación por identidad (adlfs la tomaba como una conexión real). Ahora va comentada.
- `SynapseClient` reintenta el error 42109 (`The SQL pool is warming up`) como transitorio: antes la primera consulta tras un rato inactivo fallaba sin reintento.

## [0.2.0] - 2026-10-06

### Breaking

- API de tareas: `func(datalake, logger)` / `func(datalake, gx, logger)` pasa a `func(ctx: TaskContext)`.
- Migración a Great Expectations 1.x; expectativas 0.x ya no son compatibles.
- `url_logicapp_email` sale del YAML; usar variable `LOGICAPP_EMAIL_URL`.
- Python 3.10–3.14, con `azure-datalake-utils>=0.6.0`, `pandas>=2.2` y `azure-functions` 1.x (<3.13) / 2.x (>=3.13).
- `email_sender` renombrado a `notifiers` (`LogicAppNotifier`).
- `TaskContext` ya no recibe `datalake` en el constructor: `ctx.datalake` se crea al primer uso.
- Las tareas declaran sus recursos: `@task(uses=['datalake', 'synapse', 'gx'])`. Usar un recurso no declarado lanza `RecursoNoDeclarado`.
- `@task_dq` se elimina: use `@task(uses=['gx'])`. Si una tarea devuelve un `DQResult` fallido, el runner lanza `ErrorTareaCalidadDatos`; la llamada directa devuelve el `DQResult`. `fail_on_error=False` no tiene equivalente permanente: use `silenciar_dq` (`hasta` + `motivo`) en el YAML.

### Added

- `Settings` (pydantic-settings) y validación de YAML con pydantic.
- Mensajes de cola JSON (`task`, `params`, `run_id`).
- Control de reintentos con `dequeue_count` y notificación solo en el último intento. Un fallo de calidad de datos no se reintenta: se notifica en el primer intento y no ejecuta dependientes.
- Soporte Managed Identity / `DefaultAzureCredential` para el datalake.
- Cron por tarea, `depends_on`, historial en Table Storage, CLI y plantilla de proyecto (Fase B).
- Guía de migración 0.1 → 0.2.
- Soporte de Synapse SQL, serverless primero: `SynapseClient` (`ctx.synapse`, `ctx.synapse_for(base)`) con Managed Identity, `query`/`scalar`/`execute`/`read_df`/`execute_file`, plantillas `{{param}}` con lista blanca, lotes `GO` y reintentos solo ante errores transitorios. Extra opcional `[synapse]` (`pyodbc`). Variables `SYNAPSE_*`.
- Registro de plataformas (`register_resource`): `datalake`, `synapse` y `gx` se registran con la misma API pública que un proyecto cliente usa para agregar las suyas. Conexiones con nombre (`'synapse:gold'`, `ctx.resource('synapse', 'gold')`) configuradas con variables con sufijo `__<NOMBRE>`: los datos de conexión (cuenta, workspace, base, credenciales) no se heredan de la conexión por defecto; el resto (timeouts, contenedores, SAS) sí.
- Silencio temporal de alarmas de calidad (`silenciar_dq` con `hasta` y `motivo` en el YAML): no lanza ni notifica, deja log y deja correr las dependientes; vence solo y `validate-config` avisa los vencidos o por vencer.
- Verificación de configuración sin conectarse: el runner no ejecuta una tarea con recursos sin configurar (`RecursoNoConfigurado`) y el comando `check-resources` revisa todas las tareas.
- Prueba de integración con Synapse: `pytest -m synapse` y `docs/synapse.env.example`.
- Notebooks de uso por plataforma: `docs/01_datalake.ipynb`, `docs/02_great_expectations.ipynb` y `docs/03_synapse.ipynb` (reemplazan `usage_example.ipynb` y `synapse_integracion.ipynb`).

### Fixed

- Flags de entorno interpretados como booleanos reales.
- Logs duplicados en handlers reutilizados.
- Correos de DQ filtraban mal las expectativas fallidas.
- Endpoint HTTP aceptaba listas vacías y no validaba tareas.
- `ctx.gx.validate` fallaba al reutilizar la misma lista de expectativas en otra validación (GX 1.x liga cada expectativa a su suite).

### Security

- YAML de docs sanitizado; secretos deben vivir en App Settings / Key Vault.

## [0.1.3] - 2024-01-15

### Fixed

- Workflows de release y permisos de GitHub Actions.

## [0.1.2] - 2023-12-26

### Added

- URL con SAS Token para reportes de Great Expectations.
- Mejoras en formato de email.

## [0.1.1] - 2023-12-15

### Fixed

- Mensaje de excepción en email sender.
- Manejo de Queue List.

## [0.1.0] - 2023-12-10

### Added

- Primera versión MVP con decoradores de tareas, programación y alertas.
