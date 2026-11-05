# Escalar el cómputo sin escalar la Azure Function

Recomendaciones vigentes para centraal-dataframework 0.2: qué motor usar en cada caso y por qué.
Ejemplos de código: [01_datalake.ipynb](01_datalake.ipynb), [02_great_expectations.ipynb](02_great_expectations.ipynb)
y [03_synapse.ipynb](03_synapse.ipynb).

## La idea en una frase

**La Function orquesta y el motor procesa.** La Function decide qué correr, cuándo, con qué parámetros y
a quién avisar. Leer, cruzar, agregar y materializar los datos le toca al motor (hoy Synapse serverless,
a futuro BigQuery). Si el volumen crece, crece el trabajo del motor, no el de la Function.

## Por qué la Function no debe procesar datos

La Function es el lugar más caro y más limitado para procesar volumen:

| Límite | Valor | Consecuencia |
|---|---|---|
| Memoria por instancia | 1,5 GB en Consumption; 2 o 4 GB en Flex Consumption | Un CSV de pocos cientos de MB en pandas ya puede agotarla (pandas usa entre 5 y 10 veces el tamaño del archivo) |
| Duración máxima | 10 min en Consumption (5 por defecto); 60 min garantizados en Premium | Un proceso largo se corta a mitad de camino |
| Cobro | Memoria × tiempo, **incluido el tiempo que espera respuestas** | Traer datos a la Function cuesta transferencia, memoria y tiempo |

Cuando la Function procesa datos, crecer obliga a subir de plan (Premium o Dedicated), agrandar las
instancias o partir las tareas. Cuando procesa el motor, la Function sigue pequeña aunque el volumen crezca.

> ⚠️ `SYNAPSE_QUERY_TIMEOUT` vale 1800 s (30 min) por defecto, más que el máximo de Consumption (10 min).
> El plan recomendado es Premium. La plantilla de `init` usa `functionTimeout` de 1 h. En Consumption,
> baje el timeout por debajo del `functionTimeout`.

## Por qué Synapse serverless es el motor

1. **Cobra por datos procesados, no por tiempo ni por servidor encendido.** El precio de referencia es
   USD 5/TB, con un mínimo de 10 MB por consulta
   ([Microsoft Learn](https://learn.microsoft.com/azure/synapse-analytics/sql/data-processed)). No hay nada
   que dimensionar ni apagar.
2. **Escala solo.** Una consulta sobre 1 GB o sobre 1 TB se escribe igual. La Function recibe el mismo
   resultado pequeño en los dos casos.
3. **Lee el lago donde está.** Con `OPENROWSET`, vistas y tablas externas, los datos no se mueven.
4. **El costo se controla escribiendo bien el SQL:**
   - usar Parquet o Delta, que son columnares;
   - filtrar por partición;
   - pedir solo las columnas necesarias;
   - materializar con CETAS lo que se lee muchas veces.

   Además, `sp_set_data_processed_limit` pone un tope de gasto diario, semanal o mensual.
5. **Crear vistas no cuesta.** El DDL y la metadata no se cobran (salvo `CREATE STATISTICS`). Solo se paga
   cuando alguien consulta.

Una consulta que agrega 50 GB y devuelve 10 filas cuesta en Synapse lo mismo que una que devuelve los
50 GB. La diferencia la paga la Function: en el segundo caso tiene que recibir y cargar 50 GB.

## Por qué pandas, y solo para resultados pequeños

pandas se mantiene porque es el formato de intercambio de todo el stack actual:

- `ctx.synapse.read_df` devuelve pandas.
- `ctx.datalake` (`azure-datalake-utils`) lee a pandas.
- `ctx.gx.validate` valida DataFrames de pandas (y es el plan B de la calidad sobre Synapse; ver abajo).

Su lugar es **después** del motor, con datos ya reducidos:

| Uso aconsejado | Ejemplo |
|---|---|
| Dar forma a un resultado de Synapse | Pivotear un resumen para un correo o un Excel |
| Validar con GX lo que no corre en el motor | Plan B de `ctx.gx.validate_table`: una partición pequeña a pandas |
| Archivos pequeños de negocio | Catálogo u homologación en Excel. Si se cruza con tablas grandes, se sube al lago y Synapse hace el cruce |
| Datos que llegan de fuera | Respuesta de una API → Parquet en el lago → Synapse los procesa |
| Lógica que SQL no expresa | Llamar una API por registro o usar una librería de Python, sobre un subconjunto que selecciona Synapse |

**No aconsejado:** `read_df('SELECT * FROM tabla')` para luego filtrar, agrupar o cruzar en pandas. Tampoco
leer un archivo grande con `ctx.datalake` para transformarlo, ni usar `.head()` en vez de `TOP`.

**Señal de alerta:** un resultado de más de ~100 000 filas o ~100 MB en memoria. No es un límite técnico;
indica que la Function está haciendo trabajo del motor.

## Calidad de datos sobre Synapse: GX en el motor

**Plan A (por defecto):** `ctx.gx.validate_table(..., particion={'fecha': valor}, modo='sql')`. GX traduce cada
expectativa a SQL, serverless la ejecuta sobre la tabla o vista con nombre, leyendo solo la partición declarada,
y a la Function solo vuelven agregados. **Plan B:** `modo='pandas'`, que trae la partición con `read_df` y
valida en la Function. La suite es la misma en los dos modos; se cambia de plan por dataset, sin tocar las
expectativas, y nunca por cuenta de la librería: el modo pedido se ejecuta o se lanza un error.

```python
@task(uses=['gx', 'synapse'])
def calidad_partner(ctx):
    return ctx.gx.validate_table(
        ctx.synapse_for('bronze'),
        name='partner',
        tabla='esquema.vista_partner',
        particion={'fecha': ctx.params['fecha']},   # la foto del día; se verifica contra la definición de la vista
        expectations=[...],
        medir_costo=True,          # deja data_processed_mb en el DQResult y en el log
    )
```

Los cruces (un SELECT sin nombre) van por `ctx.gx.validate_query(..., modo='pandas')`; en modo sql GX no
resuelve las columnas de una consulta suelta en SQL Server y la librería lo rechaza con un error claro. Un
cruce que deba correr en el motor se publica como vista con nombre y se valida con `validate_table`.

Para que el plan A sea barato:

1. **Vistas bien tipadas.** `OPENROWSET ... WITH (...)` con tipos explícitos, en vez de dejar que
   serverless infiera `varchar(8000)`. Collation `Latin1_General_100_BIN2_UTF8` en los textos de
   Parquet, para que los filtros por string se empujen al archivo. La columna de partición expuesta
   con `filepath()` y con el mismo nombre que la carpeta (`CAST(r.filepath(1) AS date) AS [fecha]` sobre
   `.../fecha=*/`): es lo que `validate_table` verifica antes de validar.
2. **La partición la aplica la librería** con `WHERE <columna> = ?`, sin funciones sobre la columna.
   `validate_table` no usa `add_batch_definition_daily` ni similares (GX envuelve la columna en funciones
   de fecha y serverless puede dejar de podar carpetas), no sondea la tabla entera al registrar el asset
   ni lista los valores distintos de la columna. Se demostró con una vista de prueba con una carpeta
   dañada: la validación por partición pasa y `particion='completa'` falla al tocarla.
3. **Suite pensada para serverless.** `result_format='SUMMARY'` (por defecto) o `'BASIC'`; `'COMPLETE'`
   lanza consultas extra para traer filas inesperadas y vuelve a escanear. Nulos, rangos, conteos y
   sets son baratos. Unicidad o distintos sobre columnas de alta cardinalidad son caros: córralos con
   menos frecuencia. En desarrollo corra la suite completa una vez con `pytest -m synapse` y revise el
   SQL que genera GX, para detectar lo que serverless no acepte antes de producción.
4. **Costo y operación (en el workspace, fuera de la librería).** `sp_set_data_processed_limit` como
   tope diario, semanal o mensual. `CREATE STATISTICS` sobre las columnas que más se filtran.
   Managed identity en la Function (`SYNAPSE_AUTH=identity`, el valor por defecto), sin secretos de
   service principal.

**Cuándo pasar un dataset al plan B:** si `data_processed_mb` por corrida es alto, o si una
expectativa no corre en serverless. Entonces se extrae esa partición una vez a pandas
(`modo='pandas'`) y se valida ahí, con la misma suite. `data_processed_mb` sale del contador diario
`sys.dm_external_data_processed`, que es de todo el workspace: con consultas en paralelo es
aproximado. Para el detalle por consulta, use `sys.dm_exec_requests_history`.

**Driver ODBC.** El plan A usa el mismo driver que `ctx.synapse` (`SYNAPSE_ODBC_DRIVER`, por defecto
`ODBC Driver 18 for SQL Server`).
- **Plan:** use Premium con Python 3.11 o 3.12; la imagen de Functions trae el 18 desde 3.11.
- **Antes de la primera tarea:** `GET /api/diag` y `check-resources` fallan si el driver configurado no
  está instalado.
- **Si la imagen solo trae el 17:** configure `SYNAPSE_ODBC_DRIVER=ODBC Driver 17 for SQL Server`.

En septiembre de 2023 la imagen de Python 3.11 salió una vez sin driver
([issue](https://github.com/Azure/azure-functions-python-worker/issues/1317)). Por eso conviene una
prueba de disponibilidad sobre `/api/diag`.

## Por qué no polars (por ahora)

polars es más eficiente que pandas: usa varios hilos, evalúa de forma perezosa, ocupa menos memoria y
puede filtrar al leer Parquet ([docs](https://docs.pola.rs/user-guide/io/cloud-storage)). Pero todas esas
ventajas sirven para **procesar volumen dentro de la Function**, y eso es lo que esta guía evita.

- Usar polars para leer el lago directamente (`scan_parquet('abfss://…')`) no ahorra nada: el mismo
  cómputo pasa a correr, y a cobrarse, en la Function, con sus límites de memoria y tiempo.
- No encaja con el stack: Great Expectations no lo soporta como datasource y `ctx.datalake` devuelve
  pandas. Habría que convertir en cada paso.
- Es otra dependencia pesada (~30–40 MB) en el paquete de la Function.

**Recomendación:** no agregarlo a la librería. Si una tarea puntual lo necesita para un archivo mediano,
puede instalarlo en su proyecto y convertir el resultado:
`pl.DataFrame(ctx.synapse.query(...))`.

## Convención de diseño: un motor por plataforma en el contexto

Cada plataforma es un recurso distinto en `TaskContext`, con su propio cliente y su propia API:

| Declaración | Acceso | Plataforma | Estado |
|---|---|---|---|
| `uses=['datalake']` | `ctx.datalake` | Azure Data Lake (archivos) | Disponible |
| `uses=['synapse']` | `ctx.synapse`, `ctx.synapse_for(base)` | Synapse SQL serverless | Disponible |
| `uses=['gx']` | `ctx.gx` | Great Expectations (calidad) | Disponible |
| `uses=['bigquery']` | `ctx.resource('bigquery')` | Google BigQuery | Futuro |

No se busca una capa SQL común que esconda el proveedor. Cada motor expone lo que sabe hacer, y la tarea
dice explícitamente con qué plataforma trabaja. Cualquier plataforma nueva entra como un recurso más del
contexto.

**Qué se carga lo decide el código de la tarea:** `@task(uses=[...])`.

- Usar un recurso no declarado **falla** (`RecursoNoDeclarado`). Así queda explícito, por ejemplo, que
  una tarea de Synapse no lee el lago desde la Function.
- Si un recurso declarado no tiene configuración, la tarea no se ejecuta. El comando
  `check-resources` revisa todas las tareas antes de desplegar.
- Puede haber varias conexiones del mismo tipo: `'synapse:gold'` o `'datalake:curado'`, configuradas
  con variables con sufijo (`SYNAPSE_WORKSPACE__GOLD`). Los datos de conexión no se heredan de
  la conexión por defecto; el resto de la configuración sí.
- Las plataformas se agregan con `register_resource(nombre, factory, check=...)`. Es la misma API para
  las del core (`datalake`, `synapse`, `gx` y, a futuro, `bigquery`) y para las de un proyecto cliente.

## BigQuery a futuro

BigQuery on-demand sigue el mismo modelo de costo: USD 6,25 por TiB leído, con el primer TiB del mes
gratis. Solo cobra las columnas y particiones que la consulta lee
([DoiT](https://www.doit.com/blog/bigquery-cost-optimization); confirmar en la página oficial de Google Cloud).
Las recomendaciones de esta guía aplican igual: filtrar, pedir solo las columnas necesarias y agregar en
el motor, y traer solo el resultado. BigQuery tiene sus propios controles de costo:

- *dry run*, para estimar los bytes antes de ejecutar;
- `maximum_bytes_billed`, que cancela la consulta si se pasaría del límite.

Para que llegue sin fricción:

- Ponga el SQL en archivos `.sql` y ejecútelo con `execute_file`, no en strings dentro del Python.
- Separe la lógica de negocio de la tarea de la consulta, para que cambiar de motor sea cambiar el `.sql`
  y la llamada a `ctx.bigquery`.

## Fuentes

- [Cost management for serverless SQL pool — Microsoft Learn](https://learn.microsoft.com/azure/synapse-analytics/sql/data-processed)
- [Serverless SQL pool cost — Microsoft Q&A](https://learn.microsoft.com/en-us/answers/questions/491491/serverless-sql-pool-built-in-cost) (precio de referencia por TB; confirmar en la calculadora de Azure)
- [BigQuery cost optimization — DoiT](https://www.doit.com/blog/bigquery-cost-optimization)
- [Cloud storage — Polars](https://docs.pola.rs/user-guide/io/cloud-storage)
