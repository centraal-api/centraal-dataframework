"""Modulo con recursos compartidos."""
# pylint: disable=line-too-long
import os

from great_expectations.data_context import EphemeralDataContext
from azure_datalake_utils import Datalake

CONTAINER = os.environ.get("CONTENEDOR_VALIDACIONES", "calidad-datos")

configuration = {
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


datalake = Datalake.from_account_key(os.environ.get("datalake"), os.environ.get("datalake_key"))

context = EphemeralDataContext(project_config=configuration)
