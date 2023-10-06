"""Modulo de runner con las clases basicas para el framework."""
from typing import Callable


class Runner:
    """Clase que representa el runner."""

    def __init__(self) -> None:
        """Constructor."""
        self.tasks = {}

    def add_task(self, func, name):
        """Adiciona tareas."""
        self.tasks[name] = func

    def get_task(self, task_name: str) -> Callable:
        """Ejecuta tareas."""
        task_fun = self.tasks.get(task_name)
        return task_fun
