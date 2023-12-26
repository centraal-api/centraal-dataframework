"""Modulo con recursos compartidos."""
# pylint: disable=line-too-long
import logging
import os
from dataclasses import dataclass
from typing import List, Tuple

import pandas as pd
from azure_datalake_utils import Datalake
from great_expectations.checkpoint.types.checkpoint_result import CheckpointResult
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from great_expectations.data_context import EphemeralDataContext

from centraal_dataframework.utils import parse_connection_string

CONTAINER = os.environ.get("CONTENEDOR_VALIDACIONES", "calidad-datos")
DUMMY_STRING = "DefaultEndpointsProtocol=https;AccountName=na;AccountKey=key!=k;EndpointSuffix=core.windows.net"

CONFIGURATION = {
    "anonymous_usage_statistics": {
        "data_context_id": "d3f5ffb9-7c5b-4600-8d40-be39dacb29fd",
        "explicit_url": False,
        "explicit_id": True,
        "enabled": True,
        "usage_statistics_url": "https://stats.greatexpectations.io/great_expectations/v1/usage_statistics",
    },
    "checkpoint_store_name": "checkpoint_store",
    "config_version": 3,
    "data_docs_sites": {
        "datalake_site": {
            "class_name": "SiteBuilder",
            "store_backend": {
                "class_name": "TupleAzureBlobStoreBackend",
                # noqa: W605
                "container": CONTAINER,  # pylint: disable=anomalous-backslash-in-string
                "connection_string": "${AZURE_STORAGE_CONNECTION_STRING}",
            },
            "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        },
    },
    "datasources": {},
    "evaluation_parameter_store_name": "evaluation_parameter_store",
    "expectations_store_name": "expectations_AZ_store",
    "fluent_datasources": {},
    "include_rendered_content": {"globally": False, "expectation_validation_result": False, "expectation_suite": False},
    "profiler_store_name": "profiler_store",
    "stores": {
        "expectations_AZ_store": {
            "class_name": "ExpectationsStore",
            "store_backend": {
                "class_name": "TupleAzureBlobStoreBackend",
                "container": CONTAINER,
                "prefix": "expectations",
                "connection_string": "${AZURE_STORAGE_CONNECTION_STRING}",
            },
        },
        "validations_AZ_store": {
            "class_name": "ValidationsStore",
            "store_backend": {
                "class_name": "TupleAzureBlobStoreBackend",
                "container": CONTAINER,
                "prefix": "validations",
                "connection_string": "${AZURE_STORAGE_CONNECTION_STRING}",
            },
        },
        "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
        "checkpoint_store": {"class_name": "CheckpointStore", "store_backend": {"class_name": "InMemoryStoreBackend"}},
        "profiler_store": {"class_name": "ProfilerStore", "store_backend": {"class_name": "InMemoryStoreBackend"}},
    },
    "validations_store_name": "validations_AZ_store",
}


def get_datalake() -> Datalake:
    """Obtiene el datalake."""
    conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", DUMMY_STRING)
    if conn_str == DUMMY_STRING:
        logging.warning("AZURE_STORAGE_CONNECTION_STRING NO esta configurado.")
    datalake_conf = parse_connection_string(conn_str)
    return Datalake.from_account_key(datalake_conf["AccountName"], datalake_conf["AccountKey"])


def get_context() -> EphemeralDataContext:
    """Context."""
    return EphemeralDataContext(project_config=CONFIGURATION)


@dataclass
class GreatExpectationsToolKit:
    """Clase para representar toolkit de great expectations."""

    context: EphemeralDataContext
    name_task: str

    def __post_init__(self):
        """Crea el resto de elementos del toolkit."""
        name_task = self.name_task
        self.datasource = self.context.sources.add_or_update_pandas(name_or_datasource=f"{name_task}_source")
        expectation_name = f"{name_task}_expectation_suite"
        expectation_suite = self.context.add_or_update_expectation_suite(expectation_suite_name=expectation_name)
        check_point = self.context.add_or_update_checkpoint(
            name=f"{name_task}_checkpoint", expectation_suite_name=expectation_suite.name
        )

        self.suite = expectation_suite
        self.check_point = check_point

    def _get_sas_user_or_default(self, duration: int = None, unit: str = None, ip: str = None) -> Tuple[int, str, str]:
        duration = int(os.environ.get("SAS_VALOR_EXPIRACION", 7)) if duration is None else duration
        unit = os.environ.get("SAS_UNIDAD_EXPIRACION", 'day') if unit is None else unit
        ip = os.environ.get("SAS_IP") if ip is None else ip
        return duration, unit, ip

    def _get_url(
        self, resource_id, public: bool = False, duration: int = None, unit: str = None, ip: str = None
    ) -> str:
        """Obtiene url de reporte."""
        docs_site_urls_list = self.context.get_docs_sites_urls(resource_identifier=resource_id)
        url = docs_site_urls_list[0]["site_url"]
        if public:
            # TODO: el valor de z13.web es un prefijo.¿se deja hardcoded o se necesita personalizacion?
            url = url.replace("blob", "z13.web").replace("$web/", "")
        else:
            _, container_path = url.split(".net/")
            duration, unit, ip = self._get_sas_user_or_default(duration, unit, ip)
            dl = get_datalake()
            url = dl.generar_url_con_sas_token(container_path, duration, unit, ip)
        return url

    def run_expectation_file_on_df(
        self, df: pd.DataFrame, name_of_df: str, expectations_suite_name: str, public: bool = False
    ) -> Tuple[str, CheckpointResult]:
        """Ejecuta una expectativa existente."""
        data_asset = self.datasource.add_dataframe_asset(name=f"{self.datasource.name}_{name_of_df}")
        result = self.context.run_checkpoint(
            checkpoint_name=self.check_point.name,
            batch_request=data_asset.build_batch_request(dataframe=df),
            expectation_suite_name=expectations_suite_name,
        )
        return self._get_url(list(result.run_results.keys())[0], public), result

    def run_expectations_on_df(
        self, df: pd.DataFrame, name_of_df: str, expectations: List[ExpectationConfiguration], public: bool = False
    ) -> Tuple[str, CheckpointResult]:
        """Ejecuta una lista de expectativas sobre un dataframe y devuelve la URL publica."""
        # adicionar las execptativas
        for exp in expectations:
            self.suite.add_expectation(exp)

        data_asset = self.datasource.add_dataframe_asset(name=f"{self.datasource.name}_{name_of_df}")
        self.context.save_expectation_suite(self.suite)

        result = self.context.run_checkpoint(
            checkpoint_name=self.check_point.name,
            batch_request=data_asset.build_batch_request(dataframe=df),
            expectation_suite_name=self.suite.name,
        )
        return self._get_url(list(result.run_results.keys())[0], public), result
