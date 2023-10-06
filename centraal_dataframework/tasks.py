"""Modulo con las tareas."""
from typing import Callable
from centraal_dataframework.blueprints import runner


def task(func: Callable):
    """Decorador para adicionar la tarea."""
    runner.add_task(func, func.__name__)
    return func
