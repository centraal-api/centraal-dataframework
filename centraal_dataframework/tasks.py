"""Modulo con las tareas."""
from typing import Callable
from centraal_dataframework.blueprints import runner
from centraal_dataframework.blueprints import datalake


def task(func: Callable):
    """Decorador para adicionar la tarea."""

    def wrapper(*args, **kwargs):
        return func(datalake, *args, **kwargs)

    runner.add_task(wrapper, func.__name__)

    return wrapper
