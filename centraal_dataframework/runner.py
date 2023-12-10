"""Modulo de runner con las clases basicas para el framework."""
import datetime
import logging
import os
from typing import Callable, List

import yaml

from centraal_dataframework.email_sender import send_email_dq, send_email_error
from centraal_dataframework.excepciones import ErrorEnTarea, ErrorTareaCalidadDatos, TareaDuplicada, TareaNoDefinida
from centraal_dataframework.resources import DUMMY_STRING

logger = logging.getLogger(__name__)
OVERWRITE_TAREA = os.environ.get("SOBREESCRIBIR_TAREA", True)
TAREA_DEBE_ESTAR_CONF = os.environ.get("TAREA_DEBE_CONFIGURACION", True)
UTC_OFFSET = datetime.timedelta(hours=os.environ.get("UTC_OFFSET", -5))


class Runner:
    """Clase que representa el runner."""

    def __init__(self) -> None:
        """Constructor."""
        self.tasks = {}
        self._load_conf()
        if "AZURE_STORAGE_CONNECTION_STRING" not in os.environ:
            logger.warning("AZURE_STORAGE_CONNECTION_STRING No configurado!")
            os.environ["AZURE_STORAGE_CONNECTION_STRING"] = DUMMY_STRING
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

        if OVERWRITE_TAREA and check_tarea_en_conf:
            logging.warning(
                "Tarea %s sobrescrita. Si quiere cambiar este comportamiento\n"
                "use la variable de ambiente `SOBREESCRIBIR_TAREA` (valor actual = %s)",
                task_name,
                OVERWRITE_TAREA,
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
        """Obtiene la tarea."""
        task_fun = self.tasks.get(task_name)
        return task_fun

    def es_programable(self, task_name_conf: dict, fecha_ejecucion: datetime = None) -> bool:
        """Verificar que la tarea puede ser programada."""
        if fecha_ejecucion is None:
            fecha_ejecucion = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

        allowed_days = str(task_name_conf["dias"])
        allowed_hours = str(task_name_conf["horas"])

        if allowed_days == "*" and allowed_hours == "*":
            return True

        fecha_en_hora_local: datetime = fecha_ejecucion + UTC_OFFSET
        allowed_days = allowed_days.split(",")
        allowed_hours = allowed_hours.split(",")
        hora_ejecucion = str(fecha_en_hora_local.time().hour)
        dia_ejecucion = str(fecha_en_hora_local.date().weekday())

        if hora_ejecucion in allowed_hours and dia_ejecucion in allowed_days:
            return True

        if allowed_days == ["*"] and hora_ejecucion in allowed_hours:
            return True

        if allowed_hours == ["*"] and dia_ejecucion in allowed_days:
            return True

        return False

    def get_tareas_programables(self, fecha_ejecucion: datetime = None) -> List[str]:
        """Obtiene las tareas que pueden se programadas."""
        tareas = []
        for task, task_conf in self.conf["tareas"].items():
            if self.es_programable(task_conf, fecha_ejecucion):
                tareas.append(task)
        return tareas

    def get_emails(self, task_name: str = None) -> List[str]:
        """Obtener los emails."""
        if task_name is not None:
            logger.warning("Solo se sporta email generales, %s parametro ignorado.", task_name)
        return self.conf["emails_notificar"]

    def run_task(self, task_name):
        """Ejecuta la tarea."""
        func_to_execute = self.get_task(task_name)
        emails = self.get_emails(task_name)

        if func_to_execute is None:
            raise TareaNoDefinida(task_name)
        try:
            func_to_execute()
        except ErrorTareaCalidadDatos as error:
            logger.info("se presento un error, se envia correo")
            send_email_dq(self.logic_app_url, emails, error)

        except Exception as error_tarea:
            logger.error("se presento error en %s", task_name, exc_info=True)
            send_email_error(self.logic_app_url, emails, error_tarea, task_name)
            raise ErrorEnTarea(task_name) from error_tarea

    def _load_conf(self) -> dict:
        archivo = os.environ.get("YAML_CONFIGURACION", "centraal_dataframework.yaml")
        logger.info("cargando configuracion desde %s", archivo)
        try:
            with open(archivo, 'r', encoding="utf-8") as file:
                data = yaml.safe_load(file)
                self.conf = data
        except FileNotFoundError as error_file:
            logger.warning("se carga archivo dummy %s", error_file, exc_info=True)
            self.conf = {"tareas": {}, "emails_notificar": [], "url_logicapp_email": "url"}
