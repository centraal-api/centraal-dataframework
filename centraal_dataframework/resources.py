"""Recursos compartidos: Datalake y Great Expectations 1.x."""

from __future__ import annotations

import logging
import os
import tempfile
import uuid
from dataclasses import dataclass, field
from typing import Any, List, Optional, Sequence

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
        import great_expectations as gx

        suite_name = f"{self.name_task}_{name}_suite"
        suite = gx.ExpectationSuite(name=suite_name, expectations=list(expectations))
        suite = self.context.suites.add_or_update(suite)
        return self._run_suite_on_df(df, name, suite, public=public)

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

    def _run_suite_on_df(self, df: Any, name: str, suite: Any, public: bool = False) -> DQResult:
        """Ejecuta checkpoint GX 1.x sobre un dataframe."""
        import great_expectations as gx
        from great_expectations.checkpoint import UpdateDataDocsAction

        uid = uuid.uuid4().hex[:8]
        asset_name = f"{self.name_task}_{name}_{uid}"
        batch_def_name = f"{asset_name}_batch"
        asset = self.datasource.add_dataframe_asset(name=asset_name)
        batch_def = asset.add_batch_definition_whole_dataframe(batch_def_name)

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
        )
        checkpoint = self.context.checkpoints.add_or_update(checkpoint)
        result = checkpoint.run(batch_parameters={'dataframe': df})

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
