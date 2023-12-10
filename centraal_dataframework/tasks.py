"""Modulo con las tareas."""
import functools
import logging
from typing import Callable

from centraal_dataframework.blueprints import runner
from centraal_dataframework.resources import GreatExpectationsToolKit, get_context, get_datalake

STR_FMT = 'TAREA: %(name)s--%(asctime)s-%(levelname)s-%(message)s'


def task(func: Callable):
    """Decorador para adicionar la tarea."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__name__)
        c_handler = logging.StreamHandler()
        fmt_log = logging.Formatter(STR_FMT)
        c_handler.setFormatter(fmt_log)
        logger.addHandler(c_handler)
        return func(get_datalake(), logger, *args, **kwargs)

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

        gx_toolkit = GreatExpectationsToolKit(get_context(), func.__name__)

        return func(get_datalake(), gx_toolkit, logger, *args, **kwargs)

    runner.add_task(wrapper, func.__name__)

    return wrapper
