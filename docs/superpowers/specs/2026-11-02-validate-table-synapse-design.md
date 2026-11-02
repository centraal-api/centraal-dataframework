# Validación GX por tabla en Synapse serverless, con poda de particiones — Diseño

Fecha: 2026-11-02. Estado: aprobado por Felipe el 2026-11-02 (con los ajustes anotados en el texto).

## Intención y por qué

La plataforma valida calidad de datos con Great Expectations (GX) usando Synapse serverless como motor:
GX traduce la suite a SQL y Synapse devuelve agregados, sin traer filas a la Function. Las tablas de bronze
son vistas sobre carpetas del lago con la partición en la ruta (`.../fecha=*/*.parquet`), y la columna de
partición se expone en la vista con `filepath(n)`.

Hoy la librería solo ofrece `validate_query` (consulta suelta). En modo `sql` contra Synapse, GX no puede
resolver las columnas de una consulta suelta en el dialecto de SQL Server (`column_reflection_fallback` hace
`selectable.name` sobre un `TextClause`), así que toda expectativa de columna falla con un falso fallo de
calidad. Probado el 2026-11-02 con GX 1.24.0 y 1.24.1: con un table asset (tabla o vista con nombre) GX
obtiene las columnas del catálogo y el modo `sql` funciona, con el mismo resultado que pandas. Registro:
`docs/superpowers/runs/2026-11-02-checklist-pruebas.md`, sección "Prueba de hipótesis".

El caso de uso a garantizar: **usar GX como motor sobre Synapse para validar la foto de un día de una vista
de bronze leyendo solo la carpeta de esa partición**, y la tabla completa en silver y gold.

## Metas

1. `validate_table`: validar una tabla o vista con nombre en modo `sql` (GX sobre Synapse) o `pandas`,
   con una partición declarada por la tarea (bronze) o sin ella (silver, gold).
2. La partición declarada se verifica contra Synapse antes de validar. Si no es una partición real de la
   vista, o no se puede comprobar, falla con un error claro. Nunca se valida leyendo de más por descuido.
3. El filtro de partición que GX emite permite que Synapse salte carpetas. Se demuestra midiendo MB
   procesados en una prueba de integración.
4. `validate_query` en modo `sql` con expectativas de columna falla con un error claro que remite a
   `validate_table` o a `modo='pandas'`, en vez de entregar un falso fallo de calidad.
5. El motor es explícito: GX corre sobre Synapse (`sql`) o sobre pandas, según lo que pida la tarea. Si el
   modo pedido no puede ejecutarse, se lanza error; **nunca** se cambia de motor por cuenta de la librería.

## No metas

- El conteo contra el manifiesto de la ingesta. Eso es otra tarea de la plataforma.
- Arreglar GX para consultas sueltas en SQL Server, ni reportar el bug upstream dentro de este cambio.
- Particiones por más de una columna, o por rangos de fechas. Una columna, un valor por llamada.
- Pools dedicados de Synapse.

## Anti-metas (sería un fracaso aunque funcione)

- Que una validación "pase" sin haber evaluado las expectativas (fallo en vacío) o falle por una causa
  técnica reportada como fallo de calidad.
- Que una validación de bronze escanee toda la vista sin que la tarea lo haya pedido.
- Que un fallo en modo `sql` termine corriendo en pandas sin que nadie lo pida.
- Nombres de workspace, bases, esquemas o tablas del cliente en el código o en los tests.

## Restricciones

- Python 3.10 a 3.14, estilo y convenciones del repo (black 120, comillas simples, docstrings google en
  español). Textos y errores en español.
- GX 1.x fluent API (versión del `poetry.lock`: 1.24.0), SQLAlchemy 2.x, pyodbc, Synapse serverless.
- Sin tablas temporales (`#`) en el SQL que se ejecute en serverless.
- Sin secretos en logs. Por el log viajan métricas, no filas.
- El cambio es genérico: sin nombres del cliente. Los datos de prueba vienen de `.env.synapse`.

## Decisiones tomadas (y por quién)

### API: `validate_table`

Nuevo método de `GreatExpectationsToolKit`, junto a `validate_query`:

```python
ctx.gx.validate_table(
    ctx.synapse, name, tabla,                 # tabla: 'esquema.tabla' o 'base.esquema.tabla'
    expectations=None, *, suite_name=None,
    particion=None,                           # {'fecha': '2026-09-24'}: una columna, un valor
    modo='sql',                               # 'sql' | 'pandas'; sin fallback
    result_format=DEFAULT_RESULT_FORMAT, medir_costo=False, public=False,
) -> DQResult
```

- (Felipe) En bronze la tarea valida la foto de un día: pasa `particion={'<columna>': valor}` y la
  validación lee solo esa partición. Cada tarea nombra su columna; no hay convención fija.
- (Felipe) En silver y gold se valida la tabla candidata completa: sin `particion`.
- (Felipe) El valor de la partición pasa por la misma lista blanca que `render` (`_SAFE_VALUE_RE`).
- (Mi decisión) `tabla` se separa en esquema y nombre; GX recibe `table_name` y `schema_name` por
  separado. Si viene con base (`base.esquema.tabla`) debe coincidir con la base del cliente; si no,
  `ValueError` (el datasource está atado a una base).
- (Mi decisión) `DQResult` no cambia de forma. `modo` sigue siendo `'sql'` o `'pandas'`.

### Verificación de la partición (modo `sql` y `pandas` por igual)

- (Felipe) La librería no cree la partición a ciegas: si en Synapse esa columna no es una partición, o
  el filtro no se puede aplicar, falla.
- (Mi decisión, cómo) Antes de validar, se lee la definición del objeto con
  `SELECT OBJECT_DEFINITION(OBJECT_ID(?))` y se comprueba que la columna declarada esté definida a partir
  de `filepath(n)` (con o sin `CAST(... AS tipo)`), por ejemplo `CAST(r.filepath(1) AS date) AS [fecha]`,
  y que el comodín `n`-ésimo de la ruta `BULK` sea una carpeta `<columna>=*`. Se tolera espacio y
  mayúsculas; el nombre de la columna se compara sin corchetes y sin distinguir mayúsculas.
- Falla con una excepción nueva `ParticionInvalida(Exception)` en `excepciones.py` cuando:
  - la definición no se puede leer (objeto inexistente, sin permiso `VIEW DEFINITION`, o no es una vista);
  - la columna no aparece en la definición;
  - la columna no sale de `filepath(n)`, o el comodín `n` de la ruta no corresponde a `<columna>=*`.
  El mensaje dice cuál de las tres causas y qué se esperaba. Cuando la definición no se puede leer, el
  mensaje dice explícitamente que la identidad necesita `GRANT VIEW DEFINITION` sobre la vista (o la base)
  además de `db_datareader`.
- (Felipe) Requisito operativo: `OBJECT_DEFINITION` exige `VIEW DEFINITION` (o ser dueño, `ALTER` o
  `CONTROL`); `db_datareader` no lo da. La identidad que valida (la Function, o quien corra las pruebas)
  necesita `GRANT VIEW DEFINITION` en las bases con vistas particionadas. Va al README, a la guía de la
  Function y al bloque 4 del checklist junto al `CREATE USER` + `db_datareader`. Si se puede probar con una
  identidad sin privilegios, se prueba; si no, queda como riesgo por verificar en el bloque 4.
- (Mi decisión) Si la tarea **no** declara `particion` pero la vista sí está particionada por `filepath`
  (la definición tiene al menos una columna derivada de `filepath(n)` cuyo comodín es `<col>=*`), la
  librería se niega con `ParticionInvalida`, para evitar un escaneo completo por descuido. Para validar
  una vista particionada entera a propósito, la tarea pasa `particion='completa'` (cadena literal). En
  silver y gold las tablas no tienen `filepath`, así que la llamada sin `particion` pasa directo.
  Felipe aprobó la regla de negarse; la forma de pedir la tabla entera es mi elección.
- (Mi decisión) Un objeto que no sea vista (tabla externa) se trata como no particionado: se valida entero
  si no hay `particion`, y con `particion` falla porque no hay definición con `filepath`. Para distinguir
  "no es vista" de "sin permiso" se consulta `sys.objects` (visible con `db_datareader`): si el objeto
  existe y es vista pero `OBJECT_DEFINITION` devuelve NULL, el error dice que falta `VIEW DEFINITION`.
- (Mi decisión, tras la revisión) Si la vista es una vista y su definición no se puede leer, se rechaza
  **también cuando no se declara `particion`**: sin definición no se puede saber si está particionada, y
  la regla es no escanear por descuido. El error pide el `GRANT VIEW DEFINITION`. Las tablas externas
  (no vistas) se validan enteras sin leer definición.
- (Mi decisión, tras la revisión) Si la definición tiene más de una columna derivada de `filepath(n)`
  (particiones anidadas, por ejemplo `anio=*/mes=*/dia=*`) y la tarea declara solo una, se rechaza con
  `ParticionInvalida` nombrando todas las columnas de partición encontradas. Hoy ninguna vista de bronze
  está así; cuando haga falta, se amplía `particion` a varias columnas en otro cambio.
- (Mi decisión, tras la revisión) La comprobación se hace sobre la definición del objeto que se valida,
  sin seguir dependencias: una vista definida sobre otra vista no tiene `OPENROWSET` propio y se trataría
  como no particionada. Se documenta la restricción: la vista que se valida debe ser la que define el
  `OPENROWSET`. Hoy todas las vistas de bronze son directas.

### Cómo aplica GX el filtro

- Table asset (`add_table_asset(name, table_name, schema_name)`) sobre el datasource SQL que ya usa la
  librería (`add_or_update_sql` con `sqlalchemy_url()` y `sqlalchemy_kwargs()`).
- Con `particion`: `asset.add_batch_definition(nombre, partitioner=PartitionerColumnValue(column_name=col),
  validate_partitioner=False)`, y el checkpoint recibe `batch_parameters={col: valor}`. GX emite
  `WHERE <col> = ?` con el valor como parámetro enlazado, sin funciones sobre la columna. Se usa
  `validate_partitioner=False` porque la validación interna de GX solo admite particionadores por fecha
  (`extract(year/month/day)`), que son justo los que podrían impedir la poda.
- Sin `particion` (o `'completa'`): `add_batch_definition_whole_table`.
- (Medido el 2026-11-02, spike `.superpowers/sdd/CHECKLIST_PRUEBAS/spike_poda.py`, vista de bronze de
  23 MB en Parquet con una sola foto):
  - Con particionador, GX **siempre** ejecuta antes `SELECT distinct(<col>) FROM <vista> ORDER BY <col>`
    (`TableAsset._fully_specified_batch_requests` → `param_defaults`), aunque reciba el valor; el valor
    solo filtra esa lista. `validate_partitioner=False` no lo evita. Sobre la columna derivada de
    `filepath` esa consulta tardó lo mismo que un conteo de solo metadatos y costó el mínimo del contador.
  - Un valor que no está en esa lista no valida en vacío: GX lanza `NoAvailableBatchesError`. Eso es
    deseable (una foto inexistente es un error, no un éxito) y hay que traducirlo a un error claro.
  - El contador `sys.dm_external_data_processed` suma un mínimo de 10 MB **por consulta**: una validación
    GX de dos expectativas registró 40 MB (cuatro consultas). Con vistas de decenas de MB la poda no se
    puede demostrar por MB; hace falta una vista de cientos de MB con dos o más fotos.
  - Literal y parámetro enlazado devolvieron lo mismo con tiempos equivalentes; la poda con parámetro no
    quedó demostrada ni refutada por MB.
  - Si la prueba de integración de poda, cuando se pueda ejecutar, muestra que Synapse no poda con el
    filtro de GX, se detiene la implementación y se vuelve al diseño: no se acepta un fallback a consulta
    suelta ni a pandas.

### Modo `pandas` de `validate_table`

- Trae con `read_df` el `SELECT` de la tabla con el filtro `WHERE <col> = <valor>` renderizado con
  `render` (literal validado), o sin filtro, y valida con `_run_suite_on_df`. Documentar que las filas
  viajan a la Function y que es para tablas pequeñas.
- (Mi decisión) Se traen todas las columnas; la suite puede nombrar cualquiera. Quien quiera recortar
  columnas usa `validate_query` en pandas.

### `validate_query` en modo `sql`

- (Felipe) Con expectativas de columna, falla con error claro que remita a `validate_table` o a
  `modo='pandas'`. (Mi decisión) El error se lanza antes de conectarse, revisando la suite: una
  expectativa es "de columna" si su configuración tiene `column`, `column_list`, `column_A`/`column_B` o
  `column_set`. Excepción: `ValueError` con el mensaje. Con solo expectativas de tabla (conteo de filas)
  el modo `sql` sigue funcionando como hoy.
- Documentar en docstring, README y notebook que con pandas las filas de la consulta viajan a la Function.

### Sin fallback de motor

- (Felipe) GX corre sobre Synapse o sobre pandas, según `modo`. Cualquier error técnico del modo pedido
  (conexión, GX, partición) se propaga como excepción; no se reintenta en el otro modo ni se devuelve
  `DQResult(success=False)` por causas técnicas.
- (Mi decisión) Si el checkpoint de GX devuelve expectativas con `exception_info.raised_exception`
  (error técnico de una métrica, como el de las columnas), `validate_table` y `validate_query` lanzan
  `ErrorEnTarea`-compatible: una excepción nueva `ErrorMotorCalidad(Exception)` con el mensaje de GX, en
  vez de marcar la expectativa como fallida. Así un problema del motor nunca se confunde con un fallo de
  calidad ni dispara una alarma de datos.

### Pruebas

- Unitarias (SQLite, como las existentes en `tests/test_resources_gx.py`): API de `validate_table`,
  separación de `tabla`, lista blanca del valor, error de `validate_query` sql con columnas, error cuando
  el modo pedido falla (sin fallback), `ErrorMotorCalidad` ante `raised_exception`, y la verificación de
  la definición con definiciones de vista sintéticas (función pura que recibe el texto de la definición).
- (Medido el 2026-11-02 con la trampa, spikes `spike_trampa*.py` en `.superpowers/sdd/CHECKLIST_PRUEBAS/`):
  - `WHERE fecha = '<valor>'` y `WHERE fecha = ?` podan igual: los dos cuentan la foto sana sin tocar la
    carpeta con el Parquet dañado. Con `CAST(? AS date)` también.
  - `SELECT DISTINCT fecha FROM <vista>` **sí abre todos los archivos** y falla con el dañado. El
    `DISTINCT` previo de GX no se resuelve desde las carpetas: rompe el aislamiento entre fotos (una
    partición dañada o a medio escribir tumba la validación de otra sana). Por eso **no se acepta** el
    `DISTINCT` de GX; se anula (decisión abajo, aprobada por Felipe tras esta evidencia; antes lo había
    aceptado con la evidencia del contador de MB).
  - `add_table_asset` de GX ejecuta `SELECT TOP 1 1 FROM <vista>` sin filtro al registrar el asset
    (`test_connection`), y también tocó el archivo dañado. Tampoco se acepta.
  - Con un `TableAsset` propio que (1) comprueba existencia por catálogo (`OBJECT_ID`) en vez de leer
    datos y (2) no llama a `param_defaults` cuando el lote ya viene especificado, la validación de la foto
    sana en modo `sql` pasa (conteo, columna existe, sin nulos) y la forzada falla; sin `DISTINCT` ni
    `TOP 1`; el SQL lleva `WHERE fecha = ?`, sin funciones ni `#`. La tabla completa (`'completa'`) falla
    con el error del Parquet en cada métrica (`exception_info.raised_exception`), que la librería debe
    convertir en `ErrorMotorCalidad`.
- (Aprobado por Felipe) La librería define `TableAssetSynapse(TableAsset)` en
  `resources.py` (o módulo propio) con `test_connection` por catálogo y `_fully_specified_batch_requests`
  que devuelve el lote tal cual cuando `options` trae todos los `param_names` del particionador; se
  registra con `datasource._add_asset(asset)`. Dependencia de API privada de GX (`_add_asset`,
  `_fully_specified_batch_requests`, `_NoMatchDiagnostics`, `_effective_schema_name`), mitigada con
  pruebas unitarias en SQLite que fallan si GX deja de emitir lo esperado (ningún `DISTINCT`, ningún
  `TOP 1`, el `WHERE <col> = ?` presente) y un test de humo que importa esos nombres.
- (Felipe) **GX acotado en `pyproject.toml` a la línea probada: `great-expectations = ">=1.24,<1.25"`.**
  Por qué: `TableAssetSynapse` sobreescribe nombres privados de GX; una versión nueva puede cambiarlos sin
  aviso y el síntoma sería silencioso (volver a lanzar el `DISTINCT` o el `TOP 1` sobre toda la vista) o
  un fallo de import. Con la cota, actualizar GX es una decisión explícita: se sube la cota, corren las
  pruebas de humo y las de integración con la trampa, y solo entonces se publica. Va al CHANGELOG como
  cambio de dependencia y al README en la sección de Synapse.
- (Felipe) Integración real con `-m synapse` en `tests/test_synapse_integration.py`, porque SQLite no
  tiene el defecto:
  1. `validate_table` sql con partición: conteo, `expect_column_to_exist`, `not_null` y un caso forzado
     (`between` con máximo imposible) que debe fallar; mismo resultado que pandas.
  2. Poda, con una trampa en vez de MB (Felipe): el contador cobra un mínimo de 10 MB por consulta y
     Synapse lee solo las columnas que usa, así que los MB no distinguen. La vista de prueba (en el
     contenedor de pruebas, con su propio `OPENROWSET` igual a las de bronze, en un esquema de prueba)
     tiene tres carpetas: dos fotos reales (`fecha=A` y `fecha=B`) y una tercera (`fecha=C`) con un
     `.parquet` dañado. Validar la partición `B` en modo `sql` debe pasar: demuestra que ni el `DISTINCT`
     previo de GX ni las consultas de la suite tocaron la carpeta dañada. Con `particion='completa'` la
     misma validación debe fallar con `ErrorMotorCalidad` (error de lectura del Parquet), lo que
     demuestra que la trampa funciona. Se imprime el SQL generado: agregados, `WHERE <col> = ?`, sin
     funciones sobre la columna y sin `#`.
  3. `validate_table` sin `particion` sobre la vista particionada → `ParticionInvalida`;
     con una columna que no es partición → `ParticionInvalida`.
  4. `validate_query` sql con expectativa de columna → `ValueError` antes de conectar.
- Variables nuevas en `.env.synapse` y `docs/synapse.env.example`:
  `SYNAPSE_TEST_PARTITION_TABLE` (la vista de prueba con la trampa, `esquema.vista`),
  `SYNAPSE_TEST_PARTITION_COLUMN` y `SYNAPSE_TEST_PARTITION_VALUE` (la foto sana `B`). Si faltan, las
  pruebas de partición se saltan. La ruta, la tabla y la vista viven solo en `.env.synapse`, nunca en el
  repo. Lo que se cree o borre en Azure o Synapse lo ejecuta Felipe con comandos que la sesión le entrega.

### Documentación y ejemplos

- README: `validate_table` como caso principal, `validate_query` para cruces (pandas) y la regla de
  partición.
- `docs/02_great_expectations.ipynb` §5 y `docs/03_synapse.ipynb` §6: pasar a `validate_table` con
  partición en sql y pandas, conservar un ejemplo de `validate_query` en pandas, y mostrar el error de
  `validate_query` sql con columnas.
- `docs/CHECKLIST_PRUEBAS.md`: ítems de bloque 1 y 2 alineados con lo anterior; la poda se mide con las
  variables nuevas y un valor inexistente; corregir "las pruebas de Synapse se saltan" en `make test`.
- `docs/synapse.env.example`, `docs/datalake.env.example` (comentar la connection string vacía) y
  CHANGELOG `[Unreleased]`.

## Lo que queda al constructor

- Nombres internos, organización de helpers (por ejemplo `synapse.py` para leer la definición y
  `resources.py` para el toolkit), y cómo se parsea la definición (regex es suficiente).
- Si conviene cachear la definición de la vista por llamada o por tarea.
- Cómo aislar `_query_batch_definition` y el nuevo `_table_batch_definition` sin duplicar código.
- El texto exacto de los errores, siempre en español y diciendo qué hacer.
