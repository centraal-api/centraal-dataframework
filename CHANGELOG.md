# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-10-06

### Breaking

- API de tareas: `func(datalake, logger)` / `func(datalake, gx, logger)` pasa a `func(ctx: TaskContext)`.
- Migración a Great Expectations 1.x; expectativas 0.x ya no son compatibles.
- `url_logicapp_email` sale del YAML; usar variable `LOGICAPP_EMAIL_URL`.
- Python 3.10–3.14, con `azure-datalake-utils>=0.6.0`, `pandas>=2.2` y `azure-functions` 1.x (<3.13) / 2.x (>=3.13).
- `email_sender` renombrado a `notifiers` (`LogicAppNotifier`).
- `TaskContext` ya no recibe `datalake` en el constructor: `ctx.datalake` se crea al primer uso.

### Added

- `Settings` (pydantic-settings) y validación de YAML con pydantic.
- Mensajes de cola JSON (`task`, `params`, `run_id`).
- Control de reintentos con `dequeue_count` y notificación solo en el último intento.
- Soporte Managed Identity / `DefaultAzureCredential` para el datalake.
- Cron por tarea, `depends_on`, historial en Table Storage, CLI y plantilla de proyecto (Fase B).
- Guía de migración 0.1 → 0.2.
- Soporte de Synapse SQL, serverless primero: `SynapseClient` (`ctx.synapse`, `ctx.synapse_for(base)`) con Managed Identity, `query`/`scalar`/`execute`/`read_df`/`execute_file`, plantillas `{{param}}` con lista blanca, lotes `GO` y reintentos solo ante errores transitorios. Extra opcional `[synapse]` (`pyodbc`). Variables `SYNAPSE_*`.
- Prueba de integración con Synapse: notebook `docs/synapse_integracion.ipynb`, `pytest -m synapse` y `docs/synapse.env.example`.

### Fixed

- Flags de entorno interpretados como booleanos reales.
- Logs duplicados en handlers reutilizados.
- Correos de DQ filtraban mal las expectativas fallidas.
- Endpoint HTTP aceptaba listas vacías y no validaba tareas.

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
