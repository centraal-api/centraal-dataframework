# Desplegar en una Azure Function

La forma soportada es una sola: **despliegue de código en plan Premium (Elastic Premium, Linux) con Python 3.11 o
3.12, y la librería instalada desde PyPI con versión fija** (`centraal-dataframework[synapse]==<versión>`). Es lo que
genera `centraal-dataframework init`.

Desplegar la Function como contenedor propio también funciona, pero no está soportado: queda como referencia en
[DESPLIEGUE_CONTENEDOR.md](DESPLIEGUE_CONTENEDOR.md).

## Por qué Premium con Python 3.11 o 3.12

- **El driver viene en la imagen.** La imagen oficial de Functions instala el *ODBC Driver 18* desde
  Python 3.11; con 3.10 instala el 17
  ([tabla oficial](https://learn.microsoft.com/azure/azure-functions/supported-languages)).
  La plantilla permite 3.11 y 3.12, que son las versiones verificadas en los Dockerfiles públicos de las
  imágenes.
- **No hace falta un contenedor propio.** Se despliega código, igual que en Consumption.
- **Sin el límite de 10 min.** Premium garantiza 60 min por ejecución. La plantilla usa
  `functionTimeout` de 1 h, por encima de `SYNAPSE_QUERY_TIMEOUT` (30 min).
- **Por qué no Flex Consumption:** no admite contenedores ni deja instalar paquetes del sistema. Si
  una imagen saliera sin driver, no habría arreglo.
- **El riesgo que queda:** que una actualización mensual de la imagen quite el driver. Ya pasó una vez
  ([issue #1317](https://github.com/Azure/azure-functions-python-worker/issues/1317), 2023). Lo cubre
  `GET /api/diag` con una prueba de disponibilidad (paso 6).

## 1. Crear el proyecto

```bash
pip install "centraal-dataframework[synapse]==<versión>"
centraal-dataframework init ./mi-function
```

`init` genera `function_app.py`, `host.json`, `requirements.txt`, `tasks/`, `infra/main.bicep` y los ejemplos
`local.settings.json.example` y `centraal_dataframework.yaml.example`. No sobrescribe archivos que ya existan
(use `--force` para eso).

- Copie `centraal_dataframework.yaml.example` a `centraal_dataframework.yaml` y declare sus tareas.
- `requirements.txt` fija la versión con la que se generó el proyecto. Si las tareas usan Synapse, deje la línea
  con el extra `[synapse]` y borre la otra:

  ```text
  centraal-dataframework[synapse]==<versión>
  azure-functions>=1.17
  ```

Subir de versión es siempre un cambio explícito de esa línea, seguido de un redespliegue y de `GET /api/diag`
(paso 6). No use rangos (`>=`): el build remoto instalaría una versión que nadie probó.

## 2. El driver ODBC

Solo lo necesitan las tareas que declaran `uses=['synapse']`. La librería lo busca por el nombre de
`SYNAPSE_ODBC_DRIVER` (por defecto `ODBC Driver 18 for SQL Server`).

**En la Function (forma soportada) no se instala nada.** Con Python 3.11 o 3.12 la imagen de Functions ya trae el
*ODBC Driver 18*, y `infra/main.bicep` deja `SYNAPSE_ODBC_DRIVER` con ese nombre. Lo único que hay que hacer es
comprobarlo después de cada despliegue con `GET /api/diag` (paso 6): el campo `odbc_drivers` debe listar
`ODBC Driver 18 for SQL Server` y `problemas` debe venir vacío.

Si `/api/diag` responde 503 porque falta el driver:

1. Mire qué drivers lista `odbc_drivers`.
2. Si la imagen trae otra versión (por ejemplo `ODBC Driver 17 for SQL Server`, que es la de Python 3.10), ponga ese
   nombre en `SYNAPSE_ODBC_DRIVER` y vuelva a llamar a `/api/diag`. La librería se prueba con el 18; el 17 también
   acepta el token de Entra, pero no está probado aquí.
3. Si no lista ninguno, la imagen de esa versión de Python dejó de traerlo. Cambie `pythonVersion` en el bicep
   (3.11 o 3.12) y redespliegue. En despliegue de código no se pueden instalar paquetes del sistema; la salida de
   último recurso es un contenedor propio ([DESPLIEGUE_CONTENEDOR.md](DESPLIEGUE_CONTENEDOR.md)), que no está
   soportado.

**En local** sí hay que instalarlo, una vez por máquina, siguiendo la
[guía de Microsoft](https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server):

- Windows: el instalador `.msi` del *ODBC Driver 18 for SQL Server*.
- Linux o WSL: el paquete `msodbcsql18` del repositorio de Microsoft (`ACCEPT_EULA=Y apt-get install msodbcsql18`
  después de agregar el repositorio, como indica la guía).
- Verifique con `python -c "import pyodbc; print(pyodbc.drivers())"`, o con `centraal-dataframework check-resources`,
  que avisa si el driver configurado no está.

Para ver qué trae la imagen de Azure sin desplegar (con Docker):

```bash
docker run --rm mcr.microsoft.com/azure-functions/python:4-python3.12 odbcinst -q -d
```

## 3. Crear la infraestructura

`infra/main.bicep` crea:

- un plan Elastic Premium Linux (`EP1` por defecto), con **una instancia siempre encendida**, sin escalado
  elástico ni pre-calentada; las tareas corren de a una ([CAPACIDAD.md](CAPACIDAD.md));
- la Function con Python 3.12 e identidad administrada asignada por el sistema;
- la cuenta de storage del host;
- Log Analytics y Application Insights, conectados a la Function;
- las variables: build remoto, `YAML_CONFIGURACION` con ruta absoluta y las `SYNAPSE_*`.

```bash
az deployment group create -g <rg> -f infra/main.bicep \
  -p appName=<app> storageName=<storage> synapseWorkspace=<workspace>
```

Premium cobra por instancia encendida, aunque no haya ejecuciones. Con `instancias=1` se paga una sola.
Súbalo solo si el monitor de capacidad lo pide: N instancias = N tareas independientes a la vez.

Dé acceso a la identidad en cada base de Synapse que lean las tareas. Lo ejecuta un admin de Entra
del workspace:

```sql
CREATE USER [<app>] FROM EXTERNAL PROVIDER;
ALTER ROLE db_datareader ADD MEMBER [<app>];
GRANT VIEW DEFINITION TO [<app>];  -- validate_table lee la definición de la vista para verificar la partición
-- Si las vistas leen el lago con OPENROWSET sobre una fuente externa con credencial (serverless), hacen falta los dos:
GRANT ADMINISTER DATABASE BULK OPERATIONS TO [<app>];
GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[<credencial>] TO [<app>];
-- Sin ellos, el SELECT falla con el error 4834 ("You do not have permission to use the bulk load statement") aunque
-- db_datareader esté; un administrador no lo nota porque ya los tiene.
```

Si el workspace tiene firewall, agregue las IP de salida de la Function
(`az functionapp show -g <rg> -n <app> --query possibleOutboundIpAddresses`) o use integración con VNet y NAT
Gateway, para que la Function salga por una sola IP fija.

Si las tareas leen o escriben en el lago con la identidad, dele el rol *Storage Blob Data Contributor* (o *Reader*)
en esa cuenta.

## 4. Configurar

Además de lo que deja el bicep, configure en la Function las variables de sus tareas (`STORAGE_ACCOUNT_NAME`,
`SYNAPSE_DATABASE`, `LOGICAPP_EMAIL_URL`, `HISTORY_ENABLED`, …; ver el [README](../README.md)).

`YAML_CONFIGURACION` va con **ruta absoluta** (`/home/site/wwwroot/centraal_dataframework.yaml`; el bicep ya la
deja así): el worker de Python no arranca en la carpeta de la app y una ruta relativa no se encuentra.

## 5. Desplegar

```bash
func azure functionapp publish <app>
```

El build remoto (`SCM_DO_BUILD_DURING_DEPLOYMENT=true`) instala `requirements.txt` desde PyPI en la imagen Linux.
Revise en el log del build que instaló `centraal-dataframework` con la versión fijada.

Sin Azure Functions Core Tools, el mismo despliegue sale con la CLI de Azure, comprimiendo la carpeta del
proyecto: `az functionapp deployment source config-zip -g <rg> -n <app> --src app.zip --build-remote true`.

## 6. Verificar y vigilar

```bash
curl "https://<app>.azurewebsites.net/api/diag?code=<function-key>"
```

```json
{"version": "<versión>", "python": "3.12.x",
 "odbc_drivers": ["ODBC Driver 18 for SQL Server"], "problemas": {}}
```

- **200 con `problemas` vacío:** todas las tareas tienen sus recursos configurados y el driver que
  necesitan.
- **503:** `problemas` dice qué tarea falla y por qué. Por ejemplo, que el driver no está instalado,
  con la lista de los instalados (paso 2).

Deje una **prueba de disponibilidad estándar de Application Insights** apuntando a `/api/diag`, con
la function key, y una alerta si falla. Así se entera si una actualización de la imagen quita el driver
antes de que falle una tarea programada. `/api/diag` no se conecta a nada: no consume Synapse.

Revise también las trazas `capacidad` y `tarea_capacidad` del monitor ([CAPACIDAD.md](CAPACIDAD.md)) y cree la
alerta sobre las trazas `CAPACIDAD:`.

## 7. Actualizar de versión

1. Lea el [CHANGELOG](../CHANGELOG.md) de las versiones que salta, sobre todo las secciones *Breaking* y *Changed*.
2. Cambie la versión en `requirements.txt` y pruebe en local (`func start`, `/api/diag`).
3. Redespliegue y repita `/api/diag`. Debe mostrar la versión nueva.

## Cuando algo falla

- **El host arranca con cero funciones** y Application Insights solo dice `Worker failed to index functions`: el
  detalle está en el worker. Reproduzca en local con la misma imagen y llame a `loader.index_function_app` y
  `loader.process_indexed_function` del worker (`/azure-functions-host/workers/python/<versión>/LINUX/X64`).
  Una sola anotación de binding que el worker no acepte descarta todas las funciones.
- **Un error de parámetros** (una fecha que no es ISO, un identificador con `;`) no cambia entre intentos. Si la
  tarea lo lanza como `ParametrosInvalidos`, la cola lo trata como el fallo de calidad: una sola ejecución, aviso en
  el primer intento, sin dependientes y sin mensaje en `*-poison`. Un `ValueError` común sigue reintentándose hasta
  `maxDequeueCount`, como cualquier error de tarea.
- **Error 4834** (*bulk load statement*) en Synapse: faltan los dos `GRANT` de `OPENROWSET` del paso 3.
