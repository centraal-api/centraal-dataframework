"""Modulo con las tareas."""

import os
import json
import inspect
from dataclasses import dataclass, field
from typing import Callable

from centraal_dataframework.constants import TASK_PARAM_NAME


@dataclass
class TaskInfo:
    """Objeto de informacion de tarea."""

    succeeded: bool
    args: dict = field(default_factory=dict)

    def to_json(self):
        """Metodo para Json."""
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(json_str):
        """Metodo desde json."""
        data = json.loads(json_str)
        return TaskInfo(**data)


class Task:
    """Decorador para definir una tarea."""

    def __init__(self, func: Callable):
        """Debe pasarse una funcion."""
        self.func = func
        self.name = func.__name__

    def _check_func(self):
        signature = inspect.signature(self.func)
        parameters = signature.parameters

        if TASK_PARAM_NAME not in parameters:
            raise TypeError(f"El parametro debe ser llamado {TASK_PARAM_NAME}")

        param = parameters[TASK_PARAM_NAME]
        param_type = param.annotation

        if not param_type == TaskInfo:
            raise ValueError("El parametro debe ser tipo `TaskInfo`")

    def __call__(self):
        """Metodo para usar la clase como decorador."""
        self._check_func()

        return self


def alert_function(taskinfo: TaskInfo):
    """Funcion que envia una alerta."""
    print("alerting!")
    url_logic_app = os.environ.get("logic_app_url")
    args_error = TaskInfo.args
    name_task_failed = args_error["name_task_failed"]
    print(name_task_failed, "fallo!", url_logic_app, taskinfo)
