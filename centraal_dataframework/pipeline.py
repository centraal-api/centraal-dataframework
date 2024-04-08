"""Pipeline."""

import logging
import datetime
from traceback import format_exc
from typing import List, Callable

from azure.durable_functions import Blueprint, DurableOrchestrationContext

from centraal_dataframework.configuration import FrameworkConfiguration
from centraal_dataframework.configuration import PipelineConfiguration
from centraal_dataframework.utils import check_if_func_name_is_register
from centraal_dataframework.utils import check_if_func_name_is_orchestator
from centraal_dataframework.tasks import Task, TaskInfo, alert_function
from centraal_dataframework.constants import UTC_OFFSET


class Pipeline:
    """Clase que representa un pipeline."""

    def __init__(self, name: str, tasks: List[Task], framework_conf: FrameworkConfiguration = None) -> None:
        """Constructor."""
        self.tasks: List[Task] = tasks
        self.name = name
        self.route = f"start_{self.name}"
        if framework_conf is not None:
            self.config: PipelineConfiguration = framework_conf.get_pipe_config(self.name)

    def set_config(self, pipe_config: PipelineConfiguration):
        """Asigna configuración."""
        if pipe_config is None:
            raise ValueError("pipe_config debe ser `PipelineConfiguration`")
        self.config = pipe_config

    def _register_tasks(self, bp: Blueprint):

        for t in self.tasks:
            if t.name == "alert":
                raise ValueError("`alert` es una actividad del framework, por favor usar otro nombre.")

            task_in_bp = check_if_func_name_is_register(bp, t.name)

            if not task_in_bp:
                bp.activity_trigger(input_name="taskinfo", activity=t.name)(t.func)

    def __create_orchestator_fun(self) -> Callable:
        emails = self.config.emails.copy()
        url_logic_app = self.config.url_logicapp_email
        tasks = self.tasks.copy()
        pipe_name = self.name

        def orchestator(context: DurableOrchestrationContext):
            results = {}
            logging.info("### Pipeline %s inicio!", pipe_name)
            try:
                for t in tasks:
                    if not results:
                        result = yield context.call_activity(t.name, TaskInfo(True, {"some": "parameter"}))
                        results[t.name] = result

                    else:
                        results[t.name] = yield context.call_activity(t.name, result)
                    logging.info("Actividad %s termino!", t.name)

            except Exception as e:
                logging.error("### Pipeline %s en error!", pipe_name)
                yield context.call_activity(
                    "alert",
                    TaskInfo(
                        succeeded=False,
                        args={
                            "name_task_failed": t.name,
                            "task_executed": [n for n in results],
                            "error": str(e),
                            "traceback": format_exc(),
                            "emails": emails,
                            "url_logic": url_logic_app,
                        },
                    ),
                )

                raise ValueError(f"Error en pipeline {pipe_name}") from e

            logging.info("### Pipeline %s termino!", pipe_name)

            return [results]

        return orchestator

    def __create_start_fun(self, client_name: str) -> Callable:
        if " " in client_name:
            raise ValueError("client_name no puede contener espacios")
        # TODO: esta parte del codigo esta bastante "fea", sin embargo en la
        # unica solucion para poder parametrizar el nombre de una función
        # de manera dinamica.
        name_orchestastor = f"start_orchestrator_func_{self.name}"
        func_str = (
            "from azure.functions import HttpRequest, HttpResponse\n"
            "from azure.durable_functions.models import OrchestrationRuntimeStatus\n"
            "import json\n"
            "import logging\n\n"
            f"async def {name_orchestastor}(req: HttpRequest, {client_name}):\n"
            f"""\texisting_instance = await {client_name}.get_status("{self.name}")\n"""
            """\tif existing_instance.runtime_status in [OrchestrationRuntimeStatus.Completed, OrchestrationRuntimeStatus.Failed, OrchestrationRuntimeStatus.Terminated, None]:\n"""
            f"""\t\tinstance_id = await {client_name}.start_new("{self.name}", instance_id="{self.name}")\n"""
            """\t\tlogging.info("Started orchestration with ID = '%s'.", instance_id)\n"""
            f"""\t\treturn {client_name}.create_check_status_response(req, instance_id)\n"""
            "\telse:\n"
            f"""\t\tbody= {{"status": 409, "body": "Existe {self.name} ejecutandose." }}\n"""
            """\t\treturn HttpResponse(json.dumps(body), mimetype="application/json")"""
        )
        namespace = {}
        exec(func_str, namespace)
        return namespace[name_orchestastor]

    def _create_register_orchestartor(self, bp: Blueprint):
        fun_orch = self.__create_orchestator_fun()
        check_if_func_name_is_orchestator(bp, self.name)
        fun_orch.__name__ = self.name
        bp.orchestration_trigger(context_name="context", orchestration=self.name)(fun_orch)

    def _create_register_start(self, bp: Blueprint):
        name = f"client{self.name.replace('_', '')}"
        fun_start = self.__create_start_fun(client_name=name)
        bp.route(route=self.route)(bp.durable_client_input(client_name=name)(fun_start))

    def register_pipeline(self, bp: Blueprint):
        """Registra el pipeline."""
        self._register_tasks(bp)
        self._create_register_orchestartor(bp)
        self._create_register_start(bp)

    def register_alert_activity(self, bp: Blueprint):
        """Registra el alerta."""

        if check_if_func_name_is_register(bp, "alert"):
            logging.debug("Ya se ha registrado la funcion de alerta")
        else:
            bp.activity_trigger(input_name="taskinfo", activity="alert")(alert_function)

    def es_programable(self, fecha_ejecucion: datetime = None) -> bool:
        """Verificar que la tarea puede ser programada."""
        if fecha_ejecucion is None:
            fecha_ejecucion = datetime.datetime.now(tz=datetime.timezone.utc)

        allowed_days = str(self.config.dias)
        allowed_hours = str(self.config.horas)

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

    def build(self, bp: Blueprint):
        """Construye el pipeline."""
        self.register_alert_activity(bp)
        self.register_pipeline(bp)
