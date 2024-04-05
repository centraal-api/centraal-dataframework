"""Modulo de runner con las clases basicas para el framework."""

import os
import datetime
import logging
from typing import List

import requests
from azure.functions import FunctionApp, TimerRequest, HttpRequest, HttpResponse
from azure.durable_functions import Blueprint

from centraal_dataframework.resources import DUMMY_STRING
from centraal_dataframework.configuration import FrameworkConfiguration
from centraal_dataframework.pipeline import Pipeline

logger = logging.getLogger(__name__)
OVERWRITE_TAREA = os.environ.get("SOBREESCRIBIR_TAREA", True)
TAREA_DEBE_ESTAR_CONF = os.environ.get("TAREA_DEBE_CONFIGURACION", True)
UTC_OFFSET = datetime.timedelta(hours=os.environ.get("UTC_OFFSET", -5))


class Runner:
    """Clase que representa el runner."""

    def __init__(self, pipelines: List[Pipeline], conf: FrameworkConfiguration = None, bp: Blueprint = None) -> None:
        """Constructor."""

        if "AZURE_STORAGE_CONNECTION_STRING" not in os.environ:
            logger.warning("AZURE_STORAGE_CONNECTION_STRING No configurado!")
            os.environ["AZURE_STORAGE_CONNECTION_STRING"] = DUMMY_STRING

        self.pipelines = {p.name: p for p in pipelines}
        self.conf = conf
        self._check_configuration()
        self._build(bp)

        function_name = os.environ.get("host")
        self.url = f"https://{function_name}.azurewebsites.net/api"
        self.url_status = f"https://{function_name}.azurewebsites.net/runtime/webhooks/durabletask/instances"

    def _check_configuration(self):
        try:
            for name, pipe in self.pipelines.items():
                pipe_config = self.conf.get_pipe_config(name)
                pipe.set_config(pipe_config)
        except KeyError as e:
            raise ValueError(f"{name} no esta definido en el archivo de configuracion") from e

    def _build(self, bp: Blueprint):
        for name, pipe in self.pipelines.items():
            pipe_config = self.conf.get_pipe_config(name)
            pipe.set_config(pipe_config)
            pipe.build(bp)

    def get_pipeline(self, pipe_name: str) -> Pipeline:
        """Obtiene la tarea."""
        return self.pipelines.get(pipe_name)

    def get_pipelines_programables(self, fecha_ejecucion: datetime = None) -> List[str]:
        """Obtiene las tareas que pueden se programadas."""
        pipelines_to_run = []
        for name, pipe in self.pipelines.items():
            if pipe.es_programable(fecha_ejecucion):
                pipelines_to_run.append(name)
        return pipelines_to_run

    def run_pipeline(self, pipeline_name: str):
        """Ejecuta el pipeline."""
        pipeline_to_execute = self.get_pipeline(pipeline_name)

        if pipeline_to_execute is None:
            raise ValueError("Pipeline no definido.")

        response = requests.post(f"{self.url}/{pipeline_to_execute.route}")
        if response.status_code == 202:
            instance_id = response.json()["id"]
            return instance_id

        raise ValueError("Error en inicio de pipeline")

    def get_status(self, pipeline_name: str):
        response = requests.get(self.url_status)
        if response.status_code == 200:
            status = response.json()["runtimeStatus"]
            return status
        else:
            print("Error checking status")
            return None

    def register_timer_triger_func(self, app: FunctionApp):
        runner = self

        def check_and_schedule_pipelines(mytimer: TimerRequest):
            """Verifica que tareas se deben programar.

            Args:
                mytimer: objeto TimerRequest requerido por la función.
            """
            utc_timestamp = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=datetime.timezone.utc)

            if mytimer.past_due:
                logging.warning('La función se ejecuto a fuera de tiempo')

            tareas = runner.get_pipelines_programables(utc_timestamp)
            if tareas is None or not tareas:
                logger.warning("No se programaron tareas")
                return "Ningun Pipeline programado"

            for tarea in tareas:
                self.run_pipeline(tarea)

            return "Pipeline programados"

        app.schedule(schedule="0 0 * * * *", arg_name="mytimer", run_on_startup=False)(check_and_schedule_pipelines)

    def register_get_status(self, app: FunctionApp):
        runner = self

        def get_status(req: HttpRequest) -> HttpResponse:
            """Adiciona una tarea.

            Args:
                req: request de la funcion, debe tener el parametro llamado "task_name",
                para ejecutar una lista de funciones en especifico.
            """
            try:
                req_body = req.get_json()
                task_names = req_body.get('pipeline', [])

            except ValueError:
                return HttpResponse(
                    "La funcion tuvo un error",
                    status_code=400,
                )

            if task_names is None:
                return HttpResponse("No se enviaron tareas", status_code=400)
                # se ejecuta tarea con el nombre especifico sin importar el archivo de configuracion
            runner.get_status(task_names)
            return HttpResponse(f"status de tareas: {task_names}", status_code=200)

        app.route(methods=['post'])(get_status)

    def register_framework_functions(self, app: FunctionApp):
        """Registra funciones en el framework."""
        self.register_timer_triger_func(app)
        self.register_get_status(app)
