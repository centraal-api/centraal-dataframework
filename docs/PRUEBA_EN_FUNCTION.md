# Probar una versión sin publicar en una Azure Function (plan Premium)

Antes de publicar en PyPI hay que confirmar que la versión funciona en una Function real: con el driver
ODBC de la imagen, la managed identity y los tiempos del plan. Esta guía lleva la versión de trabajo del
repo a una Function en **plan Premium (Elastic Premium, Linux) con despliegue de código**, sin pasar por
PyPI.

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
  `GET /api/diag` con una prueba de disponibilidad (paso 5).

## 1. Construir la wheel

```bash
poetry version 0.3.0.dev0        # versión de trabajo; no se commitea ni se etiqueta
poetry build                     # dist/centraal_dataframework-0.3.0.dev0-py3-none-any.whl
git checkout -- pyproject.toml   # vuelve a la versión del repo
```

Una versión `.devN` nunca se confunde con una publicada, y pip no la elige por accidente.

## 2. Probar en local

**Editable:** desde el proyecto de la Function, con su propio entorno virtual:

```bash
pip install -e "../centraal-dataframework[synapse]"
az login
func start
```

Luego abra `http://localhost:7071/api/diag`. En Windows, el driver tiene que estar instalado en la
máquina.

**Con la misma imagen Linux de Azure** (requiere Docker Desktop encendido):

```bash
docker run --rm mcr.microsoft.com/azure-functions/python:4-python3.12 odbcinst -q -d
```

Debe listar `[ODBC Driver 18 for SQL Server]`. Si no aparece, no despliegue: revise la versión de
Python o la etiqueta de la imagen.

## 3. Crear la infraestructura

Si el proyecto no tiene infraestructura, `centraal-dataframework init` genera `infra/main.bicep`:

- plan Elastic Premium Linux (`EP1` por defecto), con **una instancia siempre encendida**, sin escalado
  elástico ni pre-calentada; las tareas corren de a una ([CAPACIDAD.md](CAPACIDAD.md));
- Python 3.12 e identidad administrada;
- build remoto y las variables `SYNAPSE_*`.

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
-- si las vistas leen el lago con credencial:
-- GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[<credencial>] TO [<app>];
```

Si el workspace tiene firewall, agregue las IP de salida de la Function
(`az functionapp show -g <rg> -n <app> --query outboundIpAddresses`) o use integración con VNet.

## 4. Desplegar con la wheel dentro del proyecto

1. Copie la wheel a `wheels/` en el proyecto de la Function.
2. En `requirements.txt`, en lugar de `centraal-dataframework==...`, ponga:

   ```text
   ./wheels/centraal_dataframework-0.3.0.dev0-py3-none-any.whl[synapse]
   ```

3. Despliegue: `func azure functionapp publish <app>` o el pipeline. El build remoto
   (`SCM_DO_BUILD_DURING_DEPLOYMENT=true`) instala la wheel en la imagen Linux igual que un paquete de
   PyPI. No necesita git, credenciales ni índices extra, y siempre instala exactamente lo que se probó
   en local.

Hay alternativas a la wheel, como una rama o commit de git en `requirements.txt` o TestPyPI. Funcionan,
pero dependen de git en el build o de mezclar índices.

## 5. Verificar y vigilar

```bash
curl "https://<app>.azurewebsites.net/api/diag?code=<function-key>"
```

```json
{"version": "0.3.0.dev0", "python": "3.12.x",
 "odbc_drivers": ["ODBC Driver 18 for SQL Server"], "problemas": {}}
```

- **200 con `problemas` vacío:** todas las tareas tienen sus recursos configurados y el driver que
  necesitan.
- **503:** `problemas` dice qué tarea falla y por qué. Por ejemplo, que el driver no está instalado,
  con la lista de los instalados.

Deje una **prueba de disponibilidad estándar de Application Insights** apuntando a `/api/diag`, con
la function key, y una alerta si falla. Así se entera si una actualización de la imagen quita el driver
antes de que falle una tarea programada. `/api/diag` no se conecta a nada: no consume Synapse.

Revise también las trazas `capacidad` y `tarea_capacidad` del monitor ([CAPACIDAD.md](CAPACIDAD.md)).

## 6. Cuándo publicar

Publique cuando se cumpla todo esto en la Function Premium:

- `/api/diag` responde 200 con la versión `.dev0` y el driver 18;
- una tarea `@task(uses=['gx', 'synapse'])` con `ctx.gx.validate_table(..., particion={...}, modo='sql')` termina bien;
- el `data_processed_mb` que deja en el log es razonable para el dataset.

Entonces fije la versión con `bump2version`, mueva `[Unreleased]` del CHANGELOG y empuje el tag
`v<versión>`. `release.yml` publica en PyPI. En el proyecto de la Function, cambie la línea de la wheel
por `centraal-dataframework[synapse]==<versión>` y borre `wheels/`.
