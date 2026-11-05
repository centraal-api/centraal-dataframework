# Plan: desplegar 0.3.0 en la Function del cliente con la forma soportada

**Objetivo:** recrear la Function del cliente como la crearía cualquier usuario de la librería: proyecto generado con
`centraal-dataframework init`, despliegue de código en plan Premium y `centraal-dataframework[synapse]==0.3.0`
desde PyPI. Es la primera prueba en Azure de la forma soportada y cierra el bloque 5 de
[CHECKLIST_PRUEBAS.md](../../CHECKLIST_PRUEBAS.md).

**Dónde se ejecuta:** en el repo del cliente, no en este. Los nombres de recursos, bases y tablas del cliente se
quedan allá. Aquí solo vuelve el resultado (paso 8).

**Guía de referencia:** [DESPLIEGUE_FUNCTION.md](../../DESPLIEGUE_FUNCTION.md).

**Si algo no pasa:** el monitor de memoria (`memoria_limite_mb`) no es bloqueante. Si sigue en `null`, queda
anotado como mejora futura y se sigue. Cualquier otra falla se corrige en la librería y sale en un parche 0.3.x.

## Precondiciones

- [ ] `centraal-dataframework 0.3.0` está en PyPI (`pip index versions centraal-dataframework`).
- [ ] Hay una suscripción y un resource group acordados con el cliente para un plan EP1 (cobra mientras exista).
- [ ] Un admin de Entra del workspace de Synapse está disponible para ejecutar el SQL de permisos y la regla de
      firewall.
- [ ] Azure Functions Core Tools instaladas (`func --version`), o se usa `az functionapp deployment source
      config-zip` (guía, paso 5).

## 1. Proyecto nuevo con `init`

- [ ] En un entorno limpio: `pip install "centraal-dataframework[synapse]==0.3.0"`.
- [ ] `centraal-dataframework init <carpeta>` en una carpeta nueva del repo del cliente. Si el repo ya tiene la
      Function en contenedor, la nueva va en otra carpeta: no se mezcla.
- [ ] `requirements.txt`: dejar `centraal-dataframework[synapse]==0.3.0`, más las dependencias propias de las tareas
      del cliente.
- [ ] Pasar las tareas del cliente a `tasks/` y su `centraal_dataframework.yaml`.
- [ ] En local: `func start` y `GET http://localhost:7071/api/diag` responde 200.

## 2. Infraestructura

- [ ] `az deployment group create -g <rg> -f infra/main.bicep -p appName=<app> storageName=<storage>
      synapseWorkspace=<workspace>` termina bien.
- [ ] En el plan: *Scale out* con mínimo 1, máximo 1 y ninguna pre-calentada
      (`az functionapp show -g <rg> -n <app> --query siteConfig.preWarmedInstanceCount`).
- [ ] Application Insights creado y conectado (`APPLICATIONINSIGHTS_CONNECTION_STRING` en la Function).
- [ ] `YAML_CONFIGURACION=/home/site/wwwroot/centraal_dataframework.yaml` en la Function.
- [ ] Variables de las tareas (`STORAGE_ACCOUNT_NAME`, `SYNAPSE_DATABASE`, `HISTORY_ENABLED=true`,
      `LOGICAPP_EMAIL_URL`, …).

## 3. Synapse y red

- [ ] El admin ejecuta en cada base que leen las tareas el SQL del paso 3 de la guía, con el nombre de la app:
      `CREATE USER`, `db_datareader`, `GRANT VIEW DEFINITION` y, si las vistas usan `OPENROWSET` con credencial,
      `GRANT ADMINISTER DATABASE BULK OPERATIONS` y `GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL`.
- [ ] Firewall: las IP de `possibleOutboundIpAddresses` en el workspace, o VNet con NAT Gateway (una sola IP).
- [ ] Rol de la identidad en la cuenta del lago, si las tareas la usan.

## 4. Convivencia con la Function en contenedor

La Function anterior (forma B, corrida 5) tiene el mismo timer `check_and_schedule_task`. Con las dos encendidas,
cada tarea programada corre dos veces.

- [ ] Mientras se verifica la nueva, apagar el timer en ella: app setting
      `AzureWebJobs.check_and_schedule_task.Disabled=true`. Las pruebas se hacen encolando a mano
      (`POST /api/queue_task`).
- [ ] Al pasar la verificación (paso 6): apagar el timer en la vieja, quitar el `Disabled` en la nueva y, tras un
      día sin problemas, borrar la vieja.

## 5. Desplegar

- [ ] `func azure functionapp publish <app>` termina, y el log del build remoto muestra
      `centraal-dataframework-0.3.0` instalado.

## 6. Verificación (bloque 5 del checklist)

- [ ] `GET /api/diag?code=<key>`: 200, `version` `0.3.0`, Python 3.12.x, `ODBC Driver 18 for SQL Server` en
      `odbc_drivers`, `problemas: {}`.
- [ ] `POST /api/queue_task` con una tarea: inicio, fin y `tarea_capacidad` en Application Insights;
      `GET /api/status/<run_id>` → `success`; `GET /api/status?task=<tarea>` la lista.
- [ ] Una tarea con `validate_table(..., particion={...}, modo='sql')` termina bien; el log muestra
      `GX <nombre> [sql] tabla=... particion=... success=True data_processed_mb=...`.
- [ ] Un fallo de calidad forzado notifica en el primer intento y no se reintenta: una sola ejecución de
      `execute_tasks_inqueue`, sin cola `*-poison`, historial `failed`.
- [ ] Una partición sin datos falla por calidad (`expect_table_row_count_to_be_between`), no por motor.
- [ ] Parámetros inválidos lanzados como `ParametrosInvalidos`: una sola ejecución, aviso en el primer intento,
      historial `failed` con el motivo, sin reintentos ni `*-poison`.
- [ ] Una tarea que falla y luego termina bien con el mismo `run_id` deja el historial en `success` sin el `error`
      anterior.
- [ ] En serie: `{"task_name": ["a", "b", "c"]}` corre en orden, sin solaparse.
- [ ] El timer `monitor_capacidad` deja `capacidad {...}` cada 30 min con `memoria_limite_mb` cerca de 3500 (EP1).
      **No bloqueante:** si sale `null`, se anota como mejora futura.
- [ ] Prueba de disponibilidad sobre `/api/diag` y alerta sobre las trazas `CAPACIDAD:`.
- [ ] `sp_set_data_processed_limit` configurado en el workspace.
- [ ] Un día de operación normal (con el timer ya encendido en la nueva) sin advertencias `CAPACIDAD:`, o con
      advertencias explicadas.

## 7. Cierre en el repo del cliente

- [ ] La Function vieja apagada y borrada (paso 4).
- [ ] Inventario de lo desplegado y su HANDOVER actualizados con la Function nueva y la versión `0.3.0`.

## 8. Resultado de vuelta en este repo

- [ ] Marcar el bloque 5 de [CHECKLIST_PRUEBAS.md](../../CHECKLIST_PRUEBAS.md) con "corrida 6".
- [ ] Agregar la corrida 6 a [runs/2026-11-02-checklist-pruebas.md](../runs/2026-11-02-checklist-pruebas.md), sin
      nombres del cliente.
- [ ] Si algo falló y se arregló en la librería: parche `0.3.x` en el CHANGELOG.
