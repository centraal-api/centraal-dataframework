"""Recursos compartidos: Datalake y Great Expectations 1.x."""

from __future__ import annotations

import copy
import logging
import os
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from centraal_dataframework.excepciones import StorageNoConfigurado
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
            modo: ``'sql'`` (plan A): GX traduce cada expectativa a SQL y Synapse devuelve solo agregados.
                ``'pandas'`` (plan B): trae la consulta a la Function con ``read_df`` y valida ahí.
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
        sql = render(query, **(params or {}))

        before = synapse.data_processed_mb() if medir_costo else None
        if modo == 'pandas':
            out = self._run_suite_on_df(synapse.read_df(sql), name, suite, public=public, result_format=result_format)
        else:
            asset_name, batch_def = self._query_batch_definition(synapse, name, sql)
            out = self._run_checkpoint(asset_name, batch_def, suite, {}, public=public, result_format=result_format)
        out.modo = modo
        if before is not None:
            out.data_processed_mb = synapse.data_processed_mb() - before
        logger.info("GX %s [%s] success=%s data_processed_mb=%s", name, modo, out.success, out.data_processed_mb)
        return out

    def _query_batch_definition(self, synapse: Any, name: str, sql: str) -> tuple:
        """Asset de consulta sobre el datasource SQL de la base (uno por base y tarea)."""
        database = getattr(synapse, 'database', None) or 'default'
        datasource = self.context.data_sources.add_or_update_sql(
            name=f"{self.name_task}_synapse_{database}",
            connection_string=synapse.sqlalchemy_url(),
            kwargs=synapse.sqlalchemy_kwargs(),
        )
        asset_name = f"{self.name_task}_{name}_{uuid.uuid4().hex[:8]}"
        asset = datasource.add_query_asset(name=asset_name, query=sql)
        return asset_name, asset.add_batch_definition_whole_table(f"{asset_name}_batch")

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
            asset_name, batch_def, suite, {'dataframe': df}, public=public, result_format=result_format
        )

    def _run_checkpoint(
        self,
        asset_name: str,
        batch_def: Any,
        suite: Any,
        batch_parameters: Dict[str, Any],
        public: bool = False,
        result_format: Optional[str] = None,
    ) -> DQResult:
        """Valida un batch definition con un checkpoint (con data docs) y arma el DQResult."""
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
