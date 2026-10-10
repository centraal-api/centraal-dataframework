"""Scaffold de proyecto Azure Functions."""

from __future__ import annotations

from pathlib import Path

FUNCTION_APP = '''"""Azure Function App — Centraal Data Framework."""
import azure.functions as func

from centraal_dataframework.blueprints import framework

# Importar módulos de tareas para registrar decoradores
from tasks import logica  # noqa: F401

app = func.FunctionApp()
app.register_functions(framework)
'''

HOST_JSON = '''{
  "version": "2.0",
  "logging": {
    "applicationInsights": {
      "samplingSettings": {
        "isEnabled": true
      }
    }
  },
  "extensions": {
    "queues": {
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
from centraal_dataframework import TaskContext, task, task_dq


@task
def hello_datalake(ctx: TaskContext):
    """Ejemplo de tarea de transformación."""
    ctx.logger.info("params=%s", ctx.params)
    ctx.logger.info("hello from hello_datalake")


@task_dq
def hello_gx(ctx: TaskContext):
    """Ejemplo de tarea DQ — completar con expectativas GX 1.x."""
    ctx.logger.info("hello from hello_gx")
'''

REQUIREMENTS = '''centraal-dataframework>=0.2.0
azure-functions>=1.17
'''

_STORAGE_CONN = (
    "DefaultEndpointsProtocol=https;AccountName=${storage.name};"
    "AccountKey=${storage.listKeys().keys[0].value};EndpointSuffix=core.windows.net"
)
BICEP = f'''param location string = resourceGroup().location
param appName string
param storageName string

resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {{
  name: storageName
  location: location
  sku: {{ name: 'Standard_LRS' }}
  kind: 'StorageV2'
}}

// Y1 + Linux = Consumption. En ese plan Python 3.12 es la ultima version.
// Para 3.13 o 3.14 usar Flex Consumption, Premium o Dedicated.
resource appServicePlan 'Microsoft.Web/serverfarms@2023-01-01' = {{
  name: '${{appName}}-plan'
  location: location
  sku: {{ name: 'Y1', tier: 'Dynamic' }}
  properties: {{ reserved: true }}
}}

resource functionApp 'Microsoft.Web/sites@2023-01-01' = {{
  name: appName
  location: location
  kind: 'functionapp,linux'
  properties: {{
    serverFarmId: appServicePlan.id
    siteConfig: {{
      linuxFxVersion: 'Python|3.12'
      appSettings: [
        {{ name: 'AzureWebJobsStorage', value: '{_STORAGE_CONN}' }}
        {{ name: 'FUNCTIONS_WORKER_RUNTIME', value: 'python' }}
        {{ name: 'QUEUE_NAME', value: 'tareas' }}
        {{ name: 'LOGICAPP_EMAIL_URL', value: '' }}
      ]
    }}
  }}
}}
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
