"""Modulo de runner con las clases basicas para el framework."""
import os
from typing import Callable, List

from centraal_dataframework.excepciones import TareaDuplicada


class Runner:
    """Clase que representa el runner."""

    def __init__(self) -> None:
        """Constructor."""
        self.tasks = {}
        self.logic_app_url = os.environ.get("logic_app_url", "")
        self._load_conf()

    def add_task(self, func, task_name):
        """Adiciona tareas."""
        if task_name not in self.tasks:
            self.tasks[task_name] = func
        else:
            raise TareaDuplicada(task_name)

    def get_task(self, task_name: str) -> Callable:
        """Ejecuta tareas."""
        task_fun = self.tasks.get(task_name)
        return task_fun

    def _load_conf(task_name: str) -> List[str]:
        return []

    def get_emails(task_name: str) -> List[str]:
        return []
