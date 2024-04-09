"""Modulo de runner con las clases basicas para el framework."""

import os
import datetime
import logging
import json
from typing import List

from azure.functions import FunctionApp, TimerRequest, HttpRequest, HttpResponse
from azure.durable_functions import Blueprint, DurableOrchestrationClient
from azure.durable_functions.models import OrchestrationRuntimeStatus

from centraal_dataframework.resources import DUMMY_STRING
from centraal_dataframework.configuration import FrameworkConfiguration
from centraal_dataframework.pipeline import Pipeline
from centraal_dataframework.constants import LOCAL

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
        if function_name == LOCAL:
            self.url = "http://localhost:7071/api"
        else:
            self.url = f"https://{function_name}.azurewebsites.net/api"

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

    def register_timer_triger_func(self, app: Blueprint):
        """Metodo que registra la funcion `register_timer_triger_func`."""
        runner = self

        @app.durable_client_input(client_name="client")
        async def check_and_schedule_pipelines(mytimer: TimerRequest, client: DurableOrchestrationClient):
            """Verifica que tareas se deben programar."""
            utc_timestamp = datetime.datetime.now(tz=datetime.timezone.utc)

            if mytimer.past_due:
                logging.warning('La función se ejecuto a fuera de tiempo')

            tareas = runner.get_pipelines_programables(utc_timestamp)
            if tareas is None or not tareas:
                logger.warning("No se programaron tareas")
            else:
                for tarea in tareas:
                    instance_id = await client.start_new(tarea, instance_id=tarea)
                    response = client.create_check_status_response(None, instance_id)
                    logging.info("response: %s", response)

        app.schedule(schedule="0 0 * * * *", arg_name="mytimer", run_on_startup=False)(check_and_schedule_pipelines)

    def register_get_status(self, app: FunctionApp):
        """Registra función para obtener status."""

        @app.durable_client_input(client_name="client")
        async def get_status(req: HttpRequest, client: DurableOrchestrationClient) -> HttpResponse:
            """Adiciona una tarea."""
            try:
                task_names = req.params.get('pipelines')

            except ValueError:
                return HttpResponse(
                    "La funcion tuvo un error",
                    status_code=400,
                )
            responses = []
            if task_names is None:
                logging.info("consultando todas los estados en running")
                instances = await client.get_status_by(runtime_status=[OrchestrationRuntimeStatus.Running])
                responses = [
                    {
                        "Pipeline": instance.name,
                        "CreatedTime": instance.created_time,
                        "LastUpdatedTime": instance.last_updated_time,
                        "RuntimeStatus": instance.runtime_status.value,
                    }
                    for instance in instances
                ]

            else:
                task_names = task_names.split(",")
                for t in task_names:
                    status = await client.get_status(instance_id=t)
                    responses.append(
                        {
                            "Pipeline": status.name,
                            "CreatedTime": status.created_time,
                            "LastUpdatedTime": status.last_updated_time,
                            "RuntimeStatus": status.runtime_status.value,
                        }
                    )

            return HttpResponse(body=json.dumps(responses, default=str), status_code=200)

        app.route(methods=['get'])(get_status)

    def register_framework_functions(self, app: Blueprint):
        """Registra funciones en el framework."""
        self.register_timer_triger_func(app)
        self.register_get_status(app)
