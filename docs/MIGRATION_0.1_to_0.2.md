# Migración 0.1.x → 0.2.0

## 1. Firma de tareas

Antes:

```python
@task
def mi_tarea(datalake, logger):
    df = datalake.read_csv(...)
    logger.info("ok")

@task_dq
def validar(datalake, gx_toolkit, logger):
    url, result = gx_toolkit.run_expectations_on_df(df, "x", expectations)
```

Ahora:

```python
from centraal_dataframework import TaskContext, task, task_dq

@task
def mi_tarea(ctx: TaskContext):
    df = ctx.datalake.read_csv(...)
    ctx.logger.info("ok params=%s", ctx.params)

@task_dq
def validar(ctx: TaskContext):
    result = ctx.gx.validate(df, "x", expectations)  # DQResult
    return result  # si success=False y fail_on_error=True, se alerta solo
```

## 2. Great Expectations 1.x

Reemplazar `ExpectationConfiguration(...)` por clases `gx.expectations.*`.

`run_expectations_on_df` / `run_expectation_file_on_df` siguen como alias pero
devuelven `DQResult` (no tupla `(url, result)`).

## 3. Configuración

| Antes | Ahora |
|-------|-------|
| `url_logicapp_email` en YAML | Variable `LOGICAPP_EMAIL_URL` |
| `UTC_OFFSET=-5` | `TIMEZONE=America/Bogota` |
| `MyStorageAccountAppSetting` | `STORAGE_CONNECTION_SETTING` (default `AzureWebJobsStorage`) |

YAML mínimo:

```yaml
emails_notificar:
  - equipo@example.com
tareas:
  mi_tarea:
    dias: '*'
    horas: 8,12,20
```

## 4. Mensajes de cola

Se acepta texto plano (`mi_tarea`) y JSON:

```json
{"task": "mi_tarea", "params": {"fecha": "2024-01-01"}, "run_id": "..."}
```

## 5. Python

Requiere Python 3.10–3.14 y `azure-datalake-utils>=0.6`.

## 6. host.json

Configure `extensions.queues.maxDequeueCount` alineado con `MAX_RETRIES` (default 5).
Las notificaciones de error se envían solo en el último intento.
