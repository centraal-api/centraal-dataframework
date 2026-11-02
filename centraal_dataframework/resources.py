"""Recursos compartidos: Datalake y Great Expectations 1.x."""

from __future__ import annotations

import copy
import logging
import os
import re
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from centraal_dataframework.excepciones import ErrorMotorCalidad, StorageNoConfigurado
from centraal_dataframework.settings import Settings, get_settings
from centraal_dataframework.utils import parse_connection_string

logger = logging.getLogger(__name__)


@dataclass
class DQResult:
    """Resultado de una validación de calidad de datos."""

    success: bool
    url: str = ''
    failed: List[str] = field(default_factory=list)
    raw: Any = None
    modo: str = 'pandas'
    data_processed_mb: Optional[float] = None


def _import_datalake():
    """Import diferido de Datalake para no fallar al importar el paquete."""
    from azure_datalake_utils import Datalake

    return Datalake


def get_datalake(settings: Optional[Settings] = None) -> Any:
    """Obtiene cliente Datalake (account key o Managed Identity)."""
    Datalake = _import_datalake()
    settings = settings or get_settings()
    conn_str = settings.resolve_storage_connection_string()

    if conn_str:
        parsed = parse_connection_string(conn_str)
        account_name = parsed.get('AccountName')
        account_key = parsed.get('AccountKey')
        if account_name and account_key:
            return Datalake.from_account_key(account_name, account_key)

    account_name = settings.storage_account_name
    if not account_name and conn_str:
        account_name = parse_connection_string(conn_str).get('AccountName')

    if account_name:
        return _datalake_from_credential(account_name)

    raise StorageNoConfigurado(
        "Configure AZURE_STORAGE_CONNECTION_STRING (con AccountKey) "
        "o STORAGE_ACCOUNT_NAME + Managed Identity / DefaultAzureCredential."
    )


def _datalake_from_credential(account_name: str) -> Any:
    """Crea Datalake con DefaultAzureCredential si la librería lo soporta."""
    from azure.identity import DefaultAzureCredential

    Datalake = _import_datalake()
    credential = DefaultAzureCredential()
    if hasattr(Datalake, 'from_credential'):
        return Datalake.from_credential(account_name, credential)
    if hasattr(Datalake, 'from_token_credential'):
        return Datalake.from_token_credential(account_name, credential)
    try:
        return Datalake(account_name, credential)  # type: ignore[call-arg]
    except TypeError as exc:
        raise StorageNoConfigurado(
            "azure-datalake-utils no expone constructor con Managed Identity. "
            "Use AZURE_STORAGE_CONNECTION_STRING con AccountKey, "
            "o actualice azure-datalake-utils con from_credential()."
        ) from exc


def get_context(settings: Optional[Settings] = None):
    """Crea un DataContext efímero de Great Expectations 1.x."""
    import great_expectations as gx

    settings = settings or get_settings()
    # Preferir contexto efímero en tmp; stores Azure se configuran si hay connection string.
    root = tempfile.mkdtemp(prefix='cdf_gx_')
    context = gx.get_context(mode='ephemeral', project_root_dir=root)
    # Desactivar analytics si es posible
    try:
        context.set_analytics_enabled(False)
    except Exception:  # noqa: BLE001
        try:
            os.environ['GX_ANALYTICS_ENABLED'] = 'false'
        except Exception:  # noqa: BLE001
            pass

    conn_str = settings.resolve_storage_connection_string()
    if conn_str:
        try:
            _configure_azure_stores(context, settings, conn_str)
        except Exception:  # noqa: BLE001
            logger.warning("No se pudieron configurar stores Azure de GX; se usan stores locales.", exc_info=True)
    return context


def _configure_azure_stores(context, settings: Settings, conn_str: str) -> None:
    """Intenta configurar data docs / stores en Azure Blob (best-effort GX 1.x)."""
    # GX 1.x API varía; si falla, el toolkit sigue con docs locales.
    container = settings.validations_container
    try:
        context.add_data_docs_site(
            site_name='datalake_site',
            site_config={
                'class_name': 'SiteBuilder',
                'store_backend': {
                    'class_name': 'TupleAzureBlobStoreBackend',
                    'container': container,
                    'connection_string': conn_str,
                },
                'site_index_builder': {'class_name': 'DefaultSiteIndexBuilder'},
            },
        )
    except Exception:  # noqa: BLE001
        logger.debug("add_data_docs_site no disponible o falló", exc_info=True)


MODOS = ('sql', 'pandas')
DEFAULT_RESULT_FORMAT = 'SUMMARY'


@dataclass
class GreatExpectationsToolKit:
    """Toolkit de Great Expectations 1.x por tarea."""

    context: Any
    name_task: str
    settings: Optional[Settings] = None

    def __post_init__(self):
        """Inicializa datasource pandas."""
        self.settings = self.settings or get_settings()
        sources = self.context.data_sources
        self.datasource = sources.add_or_update_pandas(name=f"{self.name_task}_source")

    def validate(
        self,
        df: Any,
        name: str,
        expectations: Sequence[Any],
        public: bool = False,
    ) -> DQResult:
        """Valida un DataFrame contra una lista de Expectation GX 1.x."""
        return self._run_suite_on_df(df, name, self._build_suite(name, expectations), public=public)

    def _build_suite(self, name: str, expectations: Sequence[Any]) -> Any:
        """Registra una suite efímera con copias de las expectativas."""
        import great_expectations as gx

        suite_name = f"{self.name_task}_{name}_suite"
        # GX 1.x liga cada Expectation a su suite: copias para reutilizar la lista en otras validaciones.
        suite = gx.ExpectationSuite(name=suite_name, expectations=[copy.copy(e) for e in expectations])
        return self.context.suites.add_or_update(suite)

    def validate_query(
        self,
        synapse: Any,
        name: str,
        query: str,
        expectations: Optional[Sequence[Any]] = None,
        *,
        suite_name: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        modo: str = 'sql',
        result_format: str = DEFAULT_RESULT_FORMAT,
        medir_costo: bool = False,
        public: bool = False,
    ) -> DQResult:
        """Valida el resultado de una consulta de Synapse con la misma suite en cualquier modo.

        Args:
            synapse: ``SynapseClient`` (``ctx.synapse`` o ``ctx.synapse_for(base)``).
            name: Nombre de la validación (aparece en data docs y alarmas).
            query: SELECT con solo las columnas que se validan y el filtro de partición, con ``{{param}}``.
            expectations: Expectativas GX 1.x; o bien ``suite_name`` de una suite registrada.
            suite_name: Suite existente en el contexto.
            params: Valores de ``{{param}}``; pasan por la lista blanca de ``synapse.render``.
            modo: ``'sql'`` (plan A): GX traduce cada expectativa a SQL y Synapse devuelve solo agregados; para una
                consulta suelta solo admite expectativas de tabla (conteo de filas), porque GX no resuelve las
                columnas de un ``TextClause`` en SQL Server. Para expectativas de columna use ``validate_table``.
                ``'pandas'`` (plan B): trae la consulta a la Function con ``read_df`` y valida ahí: las filas
                viajan a la Function, solo para resultados pequeños. No hay fallback entre modos.
            result_format: ``'BASIC'`` o ``'SUMMARY'``. ``'COMPLETE'`` vuelve a escanear para traer filas.
            medir_costo: Guarda en el resultado el delta de MB procesados del día (aproximado: el contador
                es de todo el workspace).
            public: URL pública de data docs.
        """
        from centraal_dataframework.synapse import render

        if modo not in MODOS:
            raise ValueError(f"modo debe ser uno de {MODOS}, no {modo!r}")
        if suite_name:
            suite = self.context.suites.get(suite_name)
        elif expectations:
            suite = self._build_suite(name, expectations)
        else:
            raise ValueError("validate_query necesita expectations o suite_name")
        if modo == 'sql':
            de_columna = _expectativas_de_columna(suite)
            if de_columna:
                raise ValueError(
                    f"validate_query en modo 'sql' no admite expectativas de columna {de_columna}: "
                    "use validate_table(..., modo='sql') o modo='pandas' "
                    "(las filas de la consulta viajan a la Function)"
                )
        sql = render(query, **(params or {}))

        before = synapse.data_processed_mb() if medir_costo else None
        if modo == 'pandas':
            out = self._run_suite_on_df(synapse.read_df(sql), name, suite, public=public, result_format=result_format)
        else:
            asset_name, batch_def = self._query_batch_definition(synapse, name, sql)
            out = self._run_checkpoint(
                asset_name, batch_def, suite, {}, public=public, result_format=result_format, name=name, modo='sql'
            )
        out.modo = modo
        if before is not None:
            out.data_processed_mb = synapse.data_processed_mb() - before
        logger.info("GX %s [%s] success=%s data_processed_mb=%s", name, modo, out.success, out.data_processed_mb)
        return out

    def validate_table(
        self,
        synapse: Any,
        name: str,
        tabla: str,
        expectations: Optional[Sequence[Any]] = None,
        *,
        suite_name: Optional[str] = None,
        particion: Union[None, str, Dict[str, Any]] = None,
        modo: str = 'sql',
        result_format: str = DEFAULT_RESULT_FORMAT,
        medir_costo: bool = False,
        public: bool = False,
    ) -> DQResult:
        """Valida una tabla o vista con nombre, entera o una partición, con GX sobre Synapse o sobre pandas.

        Es el caso principal de la plataforma. En bronze la vista acumula fotos particionadas por carpeta
        (``.../fecha=*/``) y la tarea valida la de un día: ``particion={'fecha': '2026-01-31'}``. En silver y gold la
        tabla candidata ya es un solo lote y se valida sin ``particion``.

        La partición declarada se verifica contra la definición de la vista (``OBJECT_DEFINITION``): la columna
        debe salir de ``filepath(n)`` y ``n`` ser la carpeta ``<columna>=*``. Si la vista está particionada y no se
        declara ``particion``, se rechaza para no escanear por descuido; ``particion='completa'`` la valida entera a
        propósito. Nunca se cambia de motor: si el ``modo`` pedido no puede ejecutarse, se lanza una excepción.

        Args:
            synapse: ``SynapseClient`` (``ctx.synapse`` o ``ctx.synapse_for(base)``).
            name: Nombre de la validación (aparece en data docs y alarmas).
            tabla: ``esquema.tabla`` o ``base.esquema.tabla`` (la base debe ser la del cliente).
            expectations: Expectativas GX 1.x; o bien ``suite_name`` de una suite registrada.
            suite_name: Suite existente en el contexto.
            particion: ``None`` (tabla entera; rechazado si la vista está particionada), ``'completa'`` (toda la
                vista a propósito) o ``{'columna': valor}`` con una sola columna. El valor pasa por la lista
                blanca de ``render``.
            modo: ``'sql'``: GX traduce la suite a SQL y Synapse devuelve solo agregados, leyendo solo la partición.
                ``'pandas'``: trae ``SELECT *`` de la tabla (filtrado por la partición) con ``read_df`` y valida en
                la Function: las filas viajan a la Function, solo para tablas pequeñas.
            result_format: ``'BASIC'`` o ``'SUMMARY'``.
            medir_costo: Guarda en el resultado el delta de MB procesados del día (aproximado, de todo el workspace).
            public: URL pública de data docs.

        Raises:
            ValueError: modo, suite, nombre de tabla o forma de ``particion`` inválidos.
            ParticionInvalida: el objeto no existe, la definición no se puede leer (falta ``GRANT VIEW
                DEFINITION``), la columna no es partición, o la vista está particionada y no se declaró.
            ErrorMotorCalidad: GX no pudo evaluar alguna expectativa (error técnico, no de datos).
        """
        from centraal_dataframework.excepciones import ParticionInvalida
        from centraal_dataframework.particiones import es_particionada, verificar_particion
        from centraal_dataframework.synapse import separar_tabla

        if modo not in MODOS:
            raise ValueError(f"modo debe ser uno de {MODOS}, no {modo!r}")
        if suite_name:
            suite = self.context.suites.get(suite_name)
        elif expectations:
            suite = self._build_suite(name, expectations)
        else:
            raise ValueError("validate_table necesita expectations o suite_name")
        schema, table = separar_tabla(tabla, getattr(synapse, 'database', None))
        objeto = f'{schema}.{table}'
        par = _particion_declarada(particion)

        tipo = synapse.tipo_objeto(objeto)
        if tipo is None:
            raise ParticionInvalida(objeto, "el objeto no existe o la identidad no puede verlo")
        if tipo == 'V':
            definicion = synapse.definicion_objeto(objeto)
            if definicion is None:
                raise ParticionInvalida(
                    objeto,
                    "no se puede leer la definición de la vista: la identidad necesita GRANT VIEW DEFINITION "
                    "además de db_datareader",
                )
            if par is not None:
                verificar_particion(definicion, par[0], objeto)
            elif particion is None and es_particionada(definicion):
                raise ParticionInvalida(
                    objeto,
                    "la vista está particionada por filepath(); declare particion={'columna': valor} "
                    "o particion='completa' para validarla entera a propósito",
                )
        elif par is not None:
            raise ParticionInvalida(objeto, "no es una vista con filepath(): no tiene particiones que aplicar")

        before = synapse.data_processed_mb() if medir_costo else None
        if modo == 'pandas':
            sql = f'SELECT * FROM {objeto}'
            if par is not None:
                sql += f" WHERE {par[0]} = '{par[1]}'"
            out = self._run_suite_on_df(synapse.read_df(sql), name, suite, public=public, result_format=result_format)
        else:
            asset_name, batch_def, batch_parameters = self._table_batch_definition(synapse, name, schema, table, par)
            out = self._run_checkpoint(
                asset_name,
                batch_def,
                suite,
                batch_parameters,
                public=public,
                result_format=result_format,
                name=name,
                modo='sql',
            )
        out.modo = modo
        if before is not None:
            out.data_processed_mb = synapse.data_processed_mb() - before
        logger.info(
            "GX %s [%s] tabla=%s particion=%s success=%s data_processed_mb=%s",
            name,
            modo,
            objeto,
            par[0] if par else (particion or 'ninguna'),
            out.success,
            out.data_processed_mb,
        )
        return out

    def _sql_datasource(self, synapse: Any) -> Any:
        """Datasource SQL de GX para la base del cliente (uno por base y tarea)."""
        database = getattr(synapse, 'database', None) or 'default'
        return self.context.data_sources.add_or_update_sql(
            name=f"{self.name_task}_synapse_{database}",
            connection_string=synapse.sqlalchemy_url(),
            kwargs=synapse.sqlalchemy_kwargs(),
        )

    def _query_batch_definition(self, synapse: Any, name: str, sql: str) -> tuple:
        """Asset de consulta sobre el datasource SQL de la base."""
        datasource = self._sql_datasource(synapse)
        asset_name = f"{self.name_task}_{name}_{uuid.uuid4().hex[:8]}"
        asset = datasource.add_query_asset(name=asset_name, query=sql)
        return asset_name, asset.add_batch_definition_whole_table(f"{asset_name}_batch")

    def _table_batch_definition(
        self, synapse: Any, name: str, schema: str, table: str, particion: Optional[Tuple[str, Any]]
    ) -> Tuple[str, Any, Dict[str, Any]]:
        """Asset de tabla (``TableAssetSynapse``) y su batch: entera o una partición ``(columna, valor)``."""
        from centraal_dataframework.gx_synapse import batch_definition_tabla, registrar_table_asset

        datasource = self._sql_datasource(synapse)
        asset_name = f"{self.name_task}_{name}_{uuid.uuid4().hex[:8]}"
        asset = registrar_table_asset(datasource, asset_name, schema, table)
        batch_def, batch_parameters = batch_definition_tabla(asset, particion)
        return asset_name, batch_def, batch_parameters

    def validate_suite(self, df: Any, name: str, suite_name: str, public: bool = False) -> DQResult:
        """Valida un DataFrame contra una suite existente."""
        suite = self.context.suites.get(suite_name)
        return self._run_suite_on_df(df, name, suite, public=public)

    # Compat nombres anteriores
    def run_expectations_on_df(
        self, df: Any, name_of_df: str, expectations: List[Any], public: bool = False
    ) -> DQResult:
        """Alias de validate."""
        return self.validate(df, name_of_df, expectations, public=public)

    def run_expectation_file_on_df(
        self, df: Any, name_of_df: str, expectations_suite_name: str, public: bool = False
    ) -> DQResult:
        """Alias de validate_suite."""
        return self.validate_suite(df, name_of_df, expectations_suite_name, public=public)

    def _run_suite_on_df(
        self, df: Any, name: str, suite: Any, public: bool = False, result_format: Optional[str] = None
    ) -> DQResult:
        """Ejecuta checkpoint GX 1.x sobre un dataframe."""
        asset_name = f"{self.name_task}_{name}_{uuid.uuid4().hex[:8]}"
        asset = self.datasource.add_dataframe_asset(name=asset_name)
        batch_def = asset.add_batch_definition_whole_dataframe(f"{asset_name}_batch")
        return self._run_checkpoint(
            asset_name,
            batch_def,
            suite,
            {'dataframe': df},
            public=public,
            result_format=result_format,
            name=name,
            modo='pandas',
        )

    def _run_checkpoint(
        self,
        asset_name: str,
        batch_def: Any,
        suite: Any,
        batch_parameters: Dict[str, Any],
        public: bool = False,
        result_format: Optional[str] = None,
        name: str = '',
        modo: str = '',
    ) -> DQResult:
        """Valida un batch definition con un checkpoint (con data docs) y arma el DQResult.

        Si GX no pudo evaluar alguna expectativa (``exception_info``), lanza ``ErrorMotorCalidad`` en vez de
        devolver un fallo de calidad: un problema del motor no es un problema de los datos.
        """
        import great_expectations as gx
        from great_expectations.checkpoint import UpdateDataDocsAction

        validation_def = gx.ValidationDefinition(
            name=f"{asset_name}_validation",
            data=batch_def,
            suite=suite,
        )
        validation_def = self.context.validation_definitions.add_or_update(validation_def)

        actions = []
        try:
            actions.append(UpdateDataDocsAction(name='update_data_docs'))
        except Exception:  # noqa: BLE001
            pass

        checkpoint = gx.Checkpoint(
            name=f"{asset_name}_checkpoint",
            validation_definitions=[validation_def],
            actions=actions,
            result_format=result_format or DEFAULT_RESULT_FORMAT,
        )
        checkpoint = self.context.checkpoints.add_or_update(checkpoint)
        result = checkpoint.run(batch_parameters=batch_parameters)

        mensajes = _raised_exceptions(result)
        if mensajes:
            raise ErrorMotorCalidad(name or asset_name, modo or 'pandas', mensajes)
        success = bool(getattr(result, 'success', False))
        failed = _failed_expectations(result)
        url = self._get_url(result, public=public)
        return DQResult(success=success, url=url, failed=failed, raw=result)

    def _get_url(self, result: Any, public: bool = False) -> str:
        """Obtiene URL de data docs (pública o con SAS)."""
        resource_id = None
        try:
            run_results = getattr(result, 'run_results', {}) or {}
            if run_results:
                resource_id = list(run_results.keys())[0]
        except Exception:  # noqa: BLE001
            resource_id = None

        try:
            docs = self.context.get_docs_sites_urls(resource_identifier=resource_id)
            if not docs:
                return ''
            url = docs[0]['site_url']
        except Exception:  # noqa: BLE001
            return ''

        if public and self.settings and self.settings.docs_public_base_url:
            # Reemplazar host blob por base pública configurada
            try:
                _, path = url.split('.net/', 1)
                base = self.settings.docs_public_base_url.rstrip('/')
                return f"{base}/{path.replace('$web/', '')}"
            except ValueError:
                return url

        if not public and self.settings:
            try:
                _, container_path = url.split('.net/', 1)
                dl = get_datalake(self.settings)
                return dl.generar_url_con_sas_token(
                    container_path,
                    self.settings.sas_duration,
                    self.settings.sas_unit,
                    self.settings.sas_ip,
                )
            except Exception:  # noqa: BLE001
                logger.debug("No se pudo generar SAS para docs", exc_info=True)
        return url


_IDENT_COLUMNA_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def _particion_declarada(particion: Union[None, str, Dict[str, Any]]) -> Optional[Tuple[str, str]]:
    """Normaliza ``particion``: None o 'completa' → None; ``{'col': valor}`` → ``('col', 'valor')`` validados."""
    from centraal_dataframework.synapse import _SAFE_VALUE_RE

    if particion is None or particion == 'completa':
        return None
    if not isinstance(particion, dict) or len(particion) != 1:
        raise ValueError("particion debe ser None, 'completa' o un dict con una sola columna: {'columna': valor}")
    ((columna, valor),) = particion.items()
    if not isinstance(columna, str) or not _IDENT_COLUMNA_RE.match(columna):
        raise ValueError(f"particion: nombre de columna no permitido: {columna!r}")
    texto = str(valor)
    if not _SAFE_VALUE_RE.match(texto):
        raise ValueError(f"particion: valor no permitido para {columna!r}: {texto!r}")
    return columna, texto


_KWARGS_DE_COLUMNA = ('column', 'column_list', 'column_A', 'column_B', 'column_set')


def _expectativas_de_columna(suite: Any) -> List[str]:
    """Tipos de las expectativas de la suite que nombran columnas (no se pueden evaluar en sql sobre una consulta)."""
    tipos: List[str] = []
    for exp in getattr(suite, 'expectations', []) or []:
        cfg = getattr(exp, 'configuration', None)
        kwargs = getattr(cfg, 'kwargs', None) or getattr(exp, 'kwargs', None) or {}
        if any(k in kwargs for k in _KWARGS_DE_COLUMNA):
            tipo = getattr(cfg, 'type', None) or getattr(exp, 'expectation_type', None) or type(exp).__name__
            if tipo not in tipos:
                tipos.append(str(tipo))
    return tipos


def _raised_exceptions(result: Any) -> List[str]:
    """Mensajes de las expectativas que GX no pudo evaluar (``exception_info.raised_exception``), sin repetir."""
    mensajes: List[str] = []
    run_results = getattr(result, 'run_results', None) or {}
    for value in run_results.values():
        vr = getattr(value, 'validation_result', value)
        for item in getattr(vr, 'results', []) or []:
            info = getattr(item, 'exception_info', None) or {}
            entradas = info.values() if 'raised_exception' not in info else [info]
            for entrada in entradas:
                if isinstance(entrada, dict) and entrada.get('raised_exception'):
                    mensaje = str(entrada.get('exception_message') or 'error sin mensaje')
                    if mensaje not in mensajes:
                        mensajes.append(mensaje)
    return mensajes


def _failed_expectations(result: Any) -> List[str]:
    """Lista nombres/tipos de expectativas fallidas."""
    failed: List[str] = []
    run_results = getattr(result, 'run_results', None) or {}
    for value in run_results.values():
        vr = getattr(value, 'validation_result', value)
        for item in getattr(vr, 'results', []) or []:
            if getattr(item, 'success', True) is False:
                cfg = getattr(item, 'expectation_config', None)
                failed.append(str(getattr(cfg, 'type', None) or getattr(cfg, 'expectation_type', 'expectation')))
    return failed
