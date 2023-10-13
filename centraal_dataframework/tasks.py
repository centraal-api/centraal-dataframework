"""Modulo con las tareas."""
import logging
import functools
from typing import Callable
from great_expectations.data_context import FileDataContext
from centraal_dataframework.blueprints import runner
from centraal_dataframework.resources import datalake
from centraal_dataframework.resources import context


STR_FMT = 'TAREA: %(name)s--%(asctime)s-%(levelname)s-%(message)s'


class GreatExpectationsToolKit:
    """Clase para representar toolkit de great expectations."""

    def __init__(self, source_name: str) -> None:
        """Recibe el nombre del datasource."""
        self.context: FileDataContext = context
        self.datasource = context.sources.add_pandas(name=source_name)


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


def task_dq(source_name: str = "pandas_source"):
    """Decorador para adicionar la tarea great expectations."""

    def task_dq_dec(func: Callable):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__name__)
            c_handler = logging.StreamHandler()
            fmt_log = logging.Formatter(STR_FMT)
            c_handler.setFormatter(fmt_log)
            logger.addHandler(c_handler)
            gx_toolkit = GreatExpectationsToolKit(source_name)

            return func(datalake, gx_toolkit, logger, *args, **kwargs)

        runner.add_task(wrapper, func.__name__)

        return wrapper

    return task_dq_dec
