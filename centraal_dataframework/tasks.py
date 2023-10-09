"""Modulo con las tareas."""
import logging
from typing import Callable
from centraal_dataframework.blueprints import runner
from centraal_dataframework.resources import datalake


def task(func: Callable):
    """Decorador para adicionar la tarea."""

    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__name__)
        c_handler = logging.StreamHandler()
        fmt_log = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_handler.setFormatter(fmt_log)
        logger.addHandler(c_handler)
        return func(datalake, logger, *args, **kwargs)

    runner.add_task(wrapper, func.__name__)

    return wrapper


def task_dq(func: Callable):
    """Decorador para adicionar la tarea."""

    def wrapper(*args, **kwargs):
        logger = logging.getLogger(func.__name__)
        c_handler = logging.StreamHandler()
        fmt_log = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        c_handler.setFormatter(fmt_log)
        logger.addHandler(c_handler)
        return func(datalake, logger, *args, **kwargs)

    runner.add_task(wrapper, func.__name__)

    return wrapper
