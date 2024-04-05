"""configuracion."""

import os
import logging
import datetime
from typing import List, Union, Dict

import yaml

UTC_OFFSET = datetime.timedelta(hours=os.environ.get("UTC_OFFSET", -5))
logger = logging.getLogger()
logger.setLevel(logging.INFO)


class PipelineConfiguration:
    def __init__(
        self, url_logicapp_email: str, emails: List[str], dias: Union[str, int], horas: Union[str, int]
    ) -> None:
        self.url_logicapp_email = url_logicapp_email
        self.emails: List[str] = emails
        self.dias = dias
        self.horas = horas

    def is_programable(self, date_to_execute: datetime = None) -> bool:

        if date_to_execute is None:
            date_to_execute = datetime.datetime.now(datetime.timezone.utc)

        allowed_days = str(self.dias)
        allowed_hours = str(self.horas)

        if allowed_days == "*" and allowed_hours == "*":
            return True

        fecha_en_hora_local: datetime = date_to_execute + UTC_OFFSET
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

    def __str__(self) -> str:
        return "\n".join([f"{k}:{v}" for k, v in self.__dict__.items()])

    def __repr__(self) -> str:
        return self.__str__()


class FrameworkConfiguration:

    def __init__(self) -> None:
        archivo = os.environ.get("YAML_CONFIGURACION", "centraal_dataframework.yaml")
        logger.info("cargando configuracion desde %s", archivo)
        self.pipelines_conf: Dict[str, PipelineConfiguration] = {}
        try:
            with open(archivo, 'r', encoding="utf-8") as file:
                data = yaml.safe_load(file)
                self.data = data
        except FileNotFoundError as error_file:
            logger.warning("se carga archivo dummy %s", error_file, exc_info=True)
            self.data = {"pipelines": [{}], "emails": [], "url_logicapp_email": "url"}
        self.get_config_from_yaml()

    def get_config_from_yaml(self):

        self.url_logicapp_email = self.data["url_logicapp_email"]
        self.emails = self.data["emails"]

        for name, config in self.data["pipelines"].items():
            config["url_logicapp_email"] = self.url_logicapp_email
            emails = set(self.emails).union(set(config.get("emails", [])))
            config["emails"] = list(emails)
            self.pipelines_conf[name] = PipelineConfiguration(**config)

    def get_pipe_config(self, name: str) -> PipelineConfiguration:
        return self.pipelines_conf[name]
