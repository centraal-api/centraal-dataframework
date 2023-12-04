"""Modulo de runner con las clases basicas para el framework."""
import os
import logging
from typing import Callable, List

import yaml

from centraal_dataframework.excepciones import TareaDuplicada


logger = logging.getLogger(__name__)
OVEWRITE_TAREA = os.environ.get("SOBREESCRIBIR_TAREA", True)
TAREA_DEBE_ESTAR_CONF = os.environ.get("TAREA_DEBE_CONFIGURACION", True)


class Runner:
    """Clase que representa el runner."""

    def __init__(self) -> None:
        """Constructor."""
        self.tasks = {}
        self.logic_app_url = os.environ.get("logic_app_url", "")
        self._load_conf()
        self._logic_app_url = self.conf["url_logicapp_email"]

    @property
    def logic_app_url(self) -> str:
        """Obtiene Url logic app."""
        return self._logic_app_url

    def add_task(self, func, task_name):
        """Adiciona tareas."""
        if TAREA_DEBE_ESTAR_CONF:
            check_tarea_en_conf = task_name in self.conf["tareas"]
        else:
            check_tarea_en_conf = True

        if OVEWRITE_TAREA and check_tarea_en_conf:
            logging.warning(
                "Tarea %s sobrescrita. Si quiere cambiar este comportamiento"
                "use la variable de ambiente `SOBREESCRIBIR_TAREA` (valor actual = %s)",
                task_name,
                OVEWRITE_TAREA,
            )
            self.tasks[task_name] = func

        elif not check_tarea_en_conf:
            logger.error(
                "La tarea %s debe estar definda en la configuracion. Tareas definidas: %s",
                task_name,
                self.conf["tareas"],
            )
            raise ValueError(f"{task_name} Debe estar definda en la configuración.")

        elif task_name not in self.tasks:
            self.tasks[task_name] = func
        else:
            raise TareaDuplicada(task_name)

    def get_task(self, task_name: str) -> Callable:
        """Ejecuta tareas."""
        task_fun = self.tasks.get(task_name)
        return task_fun

    def es_programable(self, task_name_conf: dict, fecha) -> bool:
        """verificar que la tarea puede ser programada."""
        return True

    def get_tareas_programables(self, fecha) -> List[str]:
        """Obtiene las tareas que pueden se programadas"""
        return [task for task, task_conf in self.conf["tareas"] if self.es_programable(task_conf, fecha)]

    def _load_conf(self) -> dict:
        archivo = os.environ.get("YAML_CONFIGURACION", "centraal_dataframework.yaml")
        logger.info("cargando configuracion desde %s", archivo)
        with open(archivo, 'r', encoding="utf-8") as file:
            data = yaml.safe_load(file)
            self.conf = data

    def get_emails(self, task_name: str = None) -> List[str]:
        """Obtener los emails."""
        if task_name is not None:
            logger.warning("En esta version se soporta email generales, %s parametro ignorado", task_name)
        return self.conf["emails_notificar"]
