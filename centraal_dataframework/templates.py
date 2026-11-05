"""Scaffold de proyecto Azure Functions."""

from __future__ import annotations

from pathlib import Path

from centraal_dataframework import __version__

FUNCTION_APP = '''"""Azure Function App — Centraal Data Framework."""
import azure.functions as func

from centraal_dataframework.blueprints import framework

# Importar módulos de tareas para registrar decoradores
from tasks import logica  # noqa: F401

app = func.FunctionApp()
app.register_functions(framework)
'''

# functionTimeout: 1 h, el máximo garantizado en Premium; queda por encima de SYNAPSE_QUERY_TIMEOUT (30 min).
# En Consumption el máximo es 10 min: este host.json es para el plan Premium de infra/main.bicep.
# Cola en serie: batchSize=1 y newBatchThreshold=0 => un mensaje (una tarea) a la vez por instancia.
# Con N instancias corren N tareas independientes a la vez (ver docs/CAPACIDAD.md).
HOST_JSON = '''{
  "version": "2.0",
  "functionTimeout": "01:00:00",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true
      }
    }
  },
  "extensions": {
    "queues": {
      "batchSize": 1,
      "newBatchThreshold": 0,
      "maxDequeueCount": 5,
      "visibilityTimeout": "00:01:00"
    }
  },
  "extensionBundle": {
    "id": "Microsoft.Azure.Functions.ExtensionBundle",
    "version": "[4.*, 5.0.0)"
  }
}
'''

LOCAL_SETTINGS = '''{
  "IsEncrypted": false,
  "Values": {
    "AzureWebJobsStorage": "UseDevelopmentStorage=true",
    "FUNCTIONS_WORKER_RUNTIME": "python",
    "QUEUE_NAME": "tareas",
    "YAML_CONFIGURACION": "centraal_dataframework.yaml",
    "LOGICAPP_EMAIL_URL": "",
    "TIMEZONE": "America/Bogota",
    "CONTENEDOR_VALIDACIONES": "calidad-datos",
    "HISTORY_ENABLED": "false",
    "MAX_RETRIES": "5"
  }
}
'''

YAML_EXAMPLE = '''emails_notificar:
  - equipo@example.com
tareas:
  hello_datalake:
    dias: '*'
    horas: 8,12,20
  hello_gx:
    dias: '*'
    horas: '*'
    depends_on: []
'''

TASKS_MOD = '''"""Ejemplo de tareas."""
from centraal_dataframework import TaskContext, task


@task(uses=['datalake'])
def hello_datalake(ctx: TaskContext):
    """Ejemplo de tarea de transformación."""
    ctx.logger.info("params=%s", ctx.params)
    ctx.logger.info("hello from hello_datalake")


@task(uses=['gx'])
def hello_gx(ctx: TaskContext):
    """Ejemplo de tarea DQ — completar con expectativas GX 1.x."""
    ctx.logger.info("hello from hello_gx")
'''

# La librería se instala desde PyPI con la versión exacta con la que se generó el proyecto.
# Subir de versión es un cambio explícito en este archivo, seguido de un redespliegue y de GET /api/diag.
REQUIREMENTS = f'''centraal-dataframework=={__version__}
# Con tareas de Synapse (uses=['synapse']), use el extra en lugar de la línea anterior:
# centraal-dataframework[synapse]=={__version__}
azure-functions>=1.17
'''

_STORAGE_CONN = (
    "DefaultEndpointsProtocol=https;AccountName=${storage.name};"
    "AccountKey=${storage.listKeys().keys[0].value};EndpointSuffix=core.windows.net"
)
BICEP = f'''// Function App en plan Premium (Elastic Premium, Linux) con despliegue de código.
// Por qué Premium: la imagen de Functions trae el ODBC Driver 18 para Python >= 3.11, sin contenedor
// propio, y el plan no tiene el límite de 10 min de Consumption. Flex Consumption no deja instalar
// paquetes del sistema si el driver faltara. Verifique después de cada despliegue: GET /api/diag.
param location string = resourceGroup().location
param appName string
param storageName string

@description('3.11 o 3.12: desde 3.11 la imagen trae ODBC Driver 18 (3.10 trae el 17).')
@allowed([ '3.11', '3.12' ])
param pythonVersion string = '3.12'

@description('Tamaño de instancia del plan Premium.')
@allowed([ 'EP1', 'EP2', 'EP3' ])
param sku string = 'EP1'

// Una instancia siempre encendida, sin escalado elástico ni pre-calentada: las tareas corren en serie
// (host.json: un mensaje a la vez) y la memoria de la instancia es para una sola tarea. Se sube solo si
// el monitor de capacidad lo pide (trazas CAPACIDAD:): N instancias = N tareas independientes a la vez.
// Ver docs/CAPACIDAD.md.
@description('Instancias siempre encendidas; también es el máximo (sin escalado elástico).')
@minValue(1)
param instancias int = 1

@description('Workspace de Synapse (vacío si las tareas no usan Synapse).')
param synapseWorkspace string = ''

resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {{
  name: storageName
  location: location
  sku: {{ name: 'Standard_LRS' }}
  kind: 'StorageV2'
  properties: {{
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }}
}}

// Application Insights: trazas de las tareas, monitor de capacidad (CAPACIDAD.md) y prueba de
// disponibilidad sobre GET /api/diag.
resource logs 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {{
  name: '${{appName}}-logs'
  location: location
  properties: {{
    sku: {{ name: 'PerGB2018' }}
    retentionInDays: 30
  }}
}}

resource insights 'Microsoft.Insights/components@2020-02-02' = {{
  name: '${{appName}}-insights'
  location: location
  kind: 'web'
  properties: {{
    Application_Type: 'web'
    WorkspaceResourceId: logs.id
  }}
}}

resource plan 'Microsoft.Web/serverfarms@2023-01-01' = {{
  name: '${{appName}}-plan'
  location: location
  kind: 'elastic'
  sku: {{ name: sku, tier: 'ElasticPremium' }}
  properties: {{
    reserved: true
    maximumElasticWorkerCount: instancias
  }}
}}

var storageConnection = '{_STORAGE_CONN}'

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {{
  name: appName
  location: location
  kind: 'functionapp,linux'
  // Identidad administrada: con ella la Function entra a Synapse y al lago sin secretos.
  identity: {{ type: 'SystemAssigned' }}
  properties: {{
    serverFarmId: plan.id
    httpsOnly: true
    siteConfig: {{
      linuxFxVersion: 'Python|${{pythonVersion}}'
      // Mínimo = máximo: nunca escala, y no hay pre-calentada porque no se crean por encima del máximo.
      minimumElasticInstanceCount: instancias
      functionAppScaleLimit: instancias
      ftpsState: 'Disabled'
      minTlsVersion: '1.2'
      appSettings: [
        {{ name: 'AzureWebJobsStorage', value: storageConnection }}
        {{ name: 'WEBSITE_CONTENTAZUREFILECONNECTIONSTRING', value: storageConnection }}
        {{ name: 'WEBSITE_CONTENTSHARE', value: toLower(appName) }}
        {{ name: 'FUNCTIONS_EXTENSION_VERSION', value: '~4' }}
        {{ name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }}
        {{ name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: insights.properties.ConnectionString }}
        // Ruta absoluta: el worker de Python no arranca en la carpeta de la app.
        {{ name: 'YAML_CONFIGURACION', value: '/home/site/wwwroot/centraal_dataframework.yaml' }}
        // Build remoto: instala requirements.txt desde PyPI en la imagen Linux.
        {{ name: 'SCM_DO_BUILD_DURING_DEPLOYMENT', value: 'true' }}
        {{ name: 'QUEUE_NAME', value: 'tareas' }}
        {{ name: 'LOGICAPP_EMAIL_URL', value: '' }}
        {{ name: 'SYNAPSE_WORKSPACE', value: synapseWorkspace }}
        {{ name: 'SYNAPSE_AUTH', value: 'identity' }}
        {{ name: 'SYNAPSE_ODBC_DRIVER', value: 'ODBC Driver 18 for SQL Server' }}
        {{ name: 'SYNAPSE_QUERY_TIMEOUT', value: '1800' }}
      ]
    }}
  }}
}}

// En cada base de Synapse que lean las tareas (lo ejecuta un admin de Entra del workspace):
//   CREATE USER [<appName>] FROM EXTERNAL PROVIDER;
//   ALTER ROLE db_datareader ADD MEMBER [<appName>];
//   GRANT VIEW DEFINITION TO [<appName>];  -- validate_table lee la definición de la vista
// Si las vistas leen el lago con OPENROWSET sobre una fuente externa con credencial, además
// (sin ellos, error 4834 "bulk load statement"):
//   GRANT ADMINISTER DATABASE BULK OPERATIONS TO [<appName>];
//   GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL::[<credencial>] TO [<appName>];
output principalId string = functionApp.identity.principalId
output functionAppName string = functionApp.name
'''

FILES = {
    'function_app.py': FUNCTION_APP,
    'host.json': HOST_JSON,
    'local.settings.json.example': LOCAL_SETTINGS,
    'centraal_dataframework.yaml.example': YAML_EXAMPLE,
    'requirements.txt': REQUIREMENTS,
    'tasks/__init__.py': '',
    'tasks/logica.py': TASKS_MOD,
    'infra/main.bicep': BICEP,
}


def scaffold(target: Path, force: bool = False) -> None:
    """Crea archivos de plantilla en target."""
    target.mkdir(parents=True, exist_ok=True)
    for rel, content in FILES.items():
        path = target / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists() and not force:
            continue
        path.write_text(content, encoding='utf-8')
