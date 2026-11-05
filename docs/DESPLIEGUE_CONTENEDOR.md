# Desplegar como contenedor (referencia, no soportado)

> **No soportado.** La forma soportada es el despliegue de código en plan Premium con la librería desde PyPI:
> [DESPLIEGUE_FUNCTION.md](DESPLIEGUE_FUNCTION.md). Esta página documenta cómo se montó la librería en una Function
> en contenedor sobre un plan Dedicado (App Service Linux) en la corrida 5 del checklist (2026-11-04, sobre el commit
> `f317ddc`, antes de 0.3.0). Las versiones nuevas no se prueban así. Úsela bajo su propia responsabilidad, por
> ejemplo cuando una imagen de Functions deje de traer el driver ODBC y haga falta instalarlo a mano.

## Qué cambia frente a la forma soportada

| | Soportada (código, Premium) | Contenedor (Dedicado) |
|---|---|---|
| Plan | Elastic Premium (EP1) | App Service Linux (B1 o mayor) |
| Qué se despliega | El código; el build remoto instala `requirements.txt` | Una imagen propia sobre la oficial de Functions |
| Driver ODBC | El de la imagen de Azure | El de la imagen base, comprobado en el `Dockerfile`; se puede instalar a mano |
| Infraestructura | `infra/main.bicep` de `init` | A mano: no hay plantilla |

## Infraestructura

- **Plan:** App Service Linux (B1 o mayor) con `capacity: 1` y `alwaysOn: true`. Sin Always On el host se duerme y
  los timers paran.
- **Function:** `linuxFxVersion: DOCKER|<imagen con digest>`, `WEBSITES_ENABLE_APP_SERVICE_STORAGE=false` y pull del
  registro con la identidad de la app (rol *AcrPull*).
- **Storage del host por identidad, sin llave:** `AzureWebJobsStorage__accountName`; con identidad asignada por el
  usuario, además `AzureWebJobsStorage__credential=managedidentity`, `AzureWebJobsStorage__clientId` y
  `AZURE_CLIENT_ID`. Roles *Storage Blob Data Owner*, *Storage Queue Data Contributor* y *Storage Table Data
  Contributor*, en una cuenta sin namespace jerárquico. Cubre la cola, el timer y el historial
  (`HISTORY_ENABLED=true` toma la cuenta de `AzureWebJobsStorage__accountName`).
- **Red:** subred propia delegada a `Microsoft.Web/serverFarms`, *Route All* y NAT Gateway; la IP del NAT es la
  única regla de la Function en el firewall de Synapse.
- **Synapse:** los mismos permisos de la identidad que en la forma soportada
  ([DESPLIEGUE_FUNCTION.md](DESPLIEGUE_FUNCTION.md), paso 3), incluidos los dos `GRANT` de `OPENROWSET`.
- `YAML_CONFIGURACION` con **ruta absoluta** (`/home/site/wwwroot/centraal_dataframework.yaml`).

## Imagen

- `requirements.txt` con la versión de PyPI fijada: `centraal-dataframework[synapse]==<versión>`. (En la corrida 5,
  antes de publicar, se usó el tarball de un commit fijado por SHA.)
- Base: `mcr.microsoft.com/azure-functions/python:4-python3.12`.
- El `Dockerfile` comprueba el driver y la construcción falla si la imagen base dejó de traerlo:

  ```dockerfile
  RUN python -c "import pyodbc, sys; sys.exit('ODBC Driver 18 for SQL Server' not in pyodbc.drivers())"
  ```

- Las pruebas de la app corren dentro de la imagen en una etapa `pruebas` (`docker build --target pruebas`), antes de
  la etapa `app`: una imagen con pruebas rotas no existe.
- Construcción con `az acr build` y referencia por digest, no por etiqueta.

## Lo que encontró la corrida 5

- `Worker failed to index functions` con cero funciones: una anotación de binding que el worker rechaza. Arreglado en
  la librería (`queue_task` usa `func.Out[List[str]]`).
- `YAML_CONFIGURACION` relativa no se encuentra: el worker no arranca en la carpeta de la app.
- Error 4834 en Synapse sin los `GRANT` de `OPENROWSET`.
- `memoria_limite_mb` en `null`: App Service no le fija límite de cgroup al contenedor. La librería ahora usa
  `MemTotal` ([CAPACIDAD.md](CAPACIDAD.md)).

Detalle de la corrida: [superpowers/runs/2026-11-02-checklist-pruebas.md](superpowers/runs/2026-11-02-checklist-pruebas.md).
