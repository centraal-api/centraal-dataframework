"""Modulo de blueprints."""
import os

import logging
import azure.functions as func
from azure_datalake_utils import Datalake

from centraal_dataframework.runner import Runner
from centraal_dataframework.excepciones import ErrorEnTarea

datalake = Datalake.from_account_key(os.environ.get("datalake"), os.environ.get("datalake_key"))
runner = Runner()
framework = func.Blueprint()


logger = logging.getLogger(__name__)
NAME_CONNECTION_STORAGE_ACCOUNT = "MyStorageAccountAppSetting"
QUEUE_NAME = "tareas"


@framework.route(methods=['post'])
@framework.queue_output(arg_name="msg", queue_name=QUEUE_NAME, connection=NAME_CONNECTION_STORAGE_ACCOUNT)
def queue_rask(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    """Lee el archivo yml y define que tareas ejecutar.

    Tambien puede usar para programar una función en especifico.

    Args:
        req: request de la funcion, puede tener un body vacio o tener un parametro
            "task_name", para ejecutar una función.

    """
    logger.info('ejecutando la funcion run_tasks')
    logger.info('leyendo el archivo .yaml de configuración')
    # TODO: leer el archivo (quizas usar pydantic?)
    try:
        req_body = req.get_json()
        task_name = req_body.get('task_name', None)

    except ValueError:
        return func.HttpResponse(
            "La funcion tuvo un error",
            status_code=400,
        )

    if task_name is not None:
        # se ejecuta tarea con el nombre especifico sin importar el archivo de configuracion
        msg.set(task_name)
    else:
        # de acuerdo al retorno del archivo yml, solo se encolan
        pass

    return func.HttpResponse("tareas programadas", status_code=200)


@framework.queue_trigger(
    arg_name="msg", queue_name=QUEUE_NAME, connection=NAME_CONNECTION_STORAGE_ACCOUNT
)  # Queue trigger
def execute_tasks_inqueue(msg: func.QueueMessage) -> None:
    """Ejecuta tareas de acuerdo al queue."""
    task_name = msg.get_body().decode('utf-8')
    logger.info('execute_tasks_queue va ejecutar la tarea: %s', task_name)
    func_to_execute = runner.get_task(task_name)
    try:
        func_to_execute()
    except Exception as error_tarea:
        logger.error("se presento error en %s", task_name, exc_info=True)
        raise ErrorEnTarea(task_name) from error_tarea
