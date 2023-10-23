"""Modulo con las tareas."""
import logging
import functools
from typing import Callable, List
from dataclasses import dataclass
import pandas as pd
from great_expectations.data_context import EphemeralDataContext
from great_expectations.core.expectation_configuration import ExpectationConfiguration
from centraal_dataframework.blueprints import runner
from centraal_dataframework.resources import datalake
from centraal_dataframework.resources import context


STR_FMT = 'TAREA: %(name)s--%(asctime)s-%(levelname)s-%(message)s'


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

    def run_expectations_on_df(
        self,
        df: pd.DataFrame,
        name_of_df: str,
        expectations: List[ExpectationConfiguration],
    ) -> str:
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

        docs_site_urls_list = self.context.get_docs_sites_urls(resource_identifier=list(result.run_results.keys())[0])
        # z13.web No estoy seguro si el prefijo se puede configurar
        url = docs_site_urls_list[0]["site_url"].replace("blob", "z13.web").replace("$web/", "")

        return url


def task(func: Callable):
    """Decorador para adicionar la tarea."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__name__)
        c_handler = logging.StreamHandler()
        fmt_log = logging.Formatter(STR_FMT)
        c_handler.setFormatter(fmt_log)
        logger.addHandler(c_handler)
        return func(datalake, logger, *args, **kwargs)

    runner.add_task(wrapper, func.__name__)

    return wrapper


def task_dq(func: Callable):
    """Decorador para adicionar la tarea great expectations."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__name__)
        c_handler = logging.StreamHandler()
        fmt_log = logging.Formatter(STR_FMT)
        c_handler.setFormatter(fmt_log)
        logger.addHandler(c_handler)

        gx_toolkit = GreatExpectationsToolKit(context, func.__name__)

        return func(datalake, gx_toolkit, logger, *args, **kwargs)

    runner.add_task(wrapper, func.__name__)

    return wrapper
