# Capacidad: una instancia, tareas en serie

La Function arranca con **una instancia Premium siempre encendida**. No tiene escalado elástico ni
instancia pre-calentada, y **ejecuta las tareas de la cola de a una**. Solo se agregan instancias, o se
cambia a una instancia más grande, cuando el monitor de capacidad lo pide.

## Por qué en serie

- **La memoria de la instancia es para una sola tarea.** Dos tareas a la vez en la misma instancia
  compiten por la memoria (EP1 tiene 3,5 GB). Una tarea de pandas puede necesitar varias veces el
  tamaño de sus datos ([MOTOR_DE_COMPUTO.md](MOTOR_DE_COMPUTO.md)).
- **La cola ya es asíncrona.** El timer encola cada hora las tareas que tocan y la instancia las
  procesa en orden, una tras otra. La serie avanza sola sin que haya que esperarla.
- **El costo es fijo y predecible.** Se paga una sola instancia.

## Cómo está configurado

| Dónde | Configuración | Efecto |
|---|---|---|
| `host.json` | `queues.batchSize: 1`, `queues.newBatchThreshold: 0` | Un mensaje (una tarea) a la vez **por instancia** |
| `infra/main.bicep` | `instancias = 1` → `minimumElasticInstanceCount`, `maximumElasticWorkerCount` y `functionAppScaleLimit` | Una instancia siempre encendida que nunca escala |
| `infra/main.bicep` | Sin `preWarmedInstanceCount` | Premium no crea instancias pre-calentadas por encima del máximo, así que con máximo 1 no hay ninguna ([doc](https://learn.microsoft.com/azure/azure-functions/functions-premium-plan)) |
| `host.json` | `functionTimeout: 01:00:00` | Una tarea, con sus dependientes, puede durar hasta 1 h |

Las tareas con `depends_on` corren en la misma ejecución que su padre, después de él. Siempre van en
serie, aunque haya más instancias.

## La regla al escalar: instancias = tareas en paralelo

El límite de un mensaje a la vez aplica **por instancia**
([doc](https://learn.microsoft.com/azure/azure-functions/functions-bindings-storage-queue)).
Por eso:

> **Con N instancias corren hasta N tareas independientes a la vez.** Cada instancia sigue tomando un
> mensaje a la vez, y cada tarea sigue teniendo toda la memoria de su instancia.

Antes de subir a N instancias, revise lo siguiente:

- **Que las tareas sean de verdad independientes.** Que no escriban el mismo archivo o tabla, y que
  ninguna lea lo que otra está produciendo. Si dos tareas tienen un orden, declárelo con `depends_on`:
  así corren juntas, en serie.
- **Que los motores aguanten N consultas a la vez.** Synapse serverless tiene su propia concurrencia y
  su tope de datos procesados (`sp_set_data_processed_limit`).
- **Que el costo se multiplica por N:** son N instancias siempre encendidas.

Para cambiarlo, redespliegue con `instancias=N`:

```bash
az deployment group create -g <rg> -f infra/main.bicep -p appName=<app> storageName=<st> instancias=2
```

## El monitor de capacidad

`monitor_capacidad` es un timer del framework que corre cada 30 min. No es una tarea de la cola: así
puede medir aunque la cola esté ocupada con una tarea larga. Deja en el log una línea con:

```text
capacidad {"mensajes_en_cola": 0, "espera_max_min": 0.0, "memoria_pico_mb": 812.4, "memoria_limite_mb": 3584.0}
```

Cada ejecución de la cola deja otra línea con lo que esperó y la memoria pico del worker:

```text
tarea_capacidad {"tarea": "carga_ventas", "run_id": "...", "espera_cola_min": 3.2, "memoria_pico_mb": 812.4}
```

Si alguna señal pasa su umbral, deja una advertencia que empieza con `CAPACIDAD:`:

| Señal | Umbral (variable, por defecto) | Recomendación |
|---|---|---|
| Mensajes esperando en la cola | `CAPACIDAD_COLA_MAX`, `5` | **Otra instancia**, si las tareas pendientes son independientes |
| Espera del mensaje más viejo | `CAPACIDAD_ESPERA_MAX_MIN`, `60` min | **Otra instancia**, si las tareas pendientes son independientes |
| Memoria pico del worker / memoria de la instancia | `CAPACIDAD_MEMORIA_PCT`, `80` % | **Instancia más grande** (EP2 o EP3), o llevar ese cómputo al motor. Más instancias no le dan más memoria a una tarea |

Cree una alerta en Application Insights sobre las trazas que contengan `CAPACIDAD:`. Una advertencia
suelta puede ser solo un pico, por ejemplo varias tareas encoladas a la misma hora. Lo que justifica
escalar es que se repita varios días.

Notas sobre las mediciones:

- **Memoria pico:** es el máximo del proceso worker desde que arrancó (`ru_maxrss`). Refleja la tarea más
  pesada desde el último reinicio, no la de ahora.
- **Memoria de la instancia:** sale del cgroup del contenedor.
- **En local (Windows) no hay datos de memoria:** esos campos quedan en `null` y no se evalúan.
- **Cola:** se lee con la misma conexión del binding (`STORAGE_CONNECTION_SETTING`, por defecto
  `AzureWebJobsStorage`). Acepta un connection string o una identidad (`<conexión>__accountName`).
  La identidad necesita el rol *Storage Queue Data Reader*.

## Cuándo escalar

1. **No escale por una alerta suelta.** Revise si se repite en varios días.
2. **Si es memoria:** primero mire qué tarea marca el pico en `tarea_capacidad`. Muchas veces se arregla
   moviendo el cómputo a Synapse. Si no, use una instancia más grande (`sku=EP2`) y no más instancias.
3. **Si es la cola:** confirme que las tareas acumuladas son independientes y suba `instancias` de a una.
   Documente el cambio y el motivo en el proyecto, con este número: **instancias = tareas que pueden
   correr a la vez**.
