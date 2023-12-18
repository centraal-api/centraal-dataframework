"""Modulo de blueprints."""
import datetime
import logging
import os
from typing import List

import azure.functions as func

from centraal_dataframework.runner import Runner

logger = logging.getLogger(__name__)
NAME_CONNECTION_STORAGE_ACCOUNT = "MyStorageAccountAppSetting"
QUEUE_NAME = os.environ.get("QUEUE_NAME", "tareas")
runner = Runner()
framework = func.Blueprint()


@framework.schedule(schedule="0 0 * * * *", arg_name="mytimer", run_on_startup=False)
@framework.queue_output(arg_name="msg", queue_name=QUEUE_NAME, connection=NAME_CONNECTION_STORAGE_ACCOUNT)
def check_and_schedule_task(mytimer: func.TimerRequest, msg: func.Out[List[str]]):
    """Verifica que tareas se deben programar.

    Args:
        mytimer: objeto TimerRequest requerido por la función.
        msg: mensaje que representa la cola en donde adicionar.
    """
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)

    if mytimer.past_due:
        logging.warning('La función se ejecuto a fuetra de tiempo')

    tareas = runner.get_tareas_programables(utc_timestamp)
    if tareas is None:
        logger.warning("No se programaron tareas")
    else:
        logging.info("se programan %s", tareas)
        msg.set(tareas)


@framework.route(methods=['post'])
@framework.queue_output(arg_name="msg", queue_name=QUEUE_NAME, connection=NAME_CONNECTION_STORAGE_ACCOUNT)
def queue_task(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    """Adiciona una tarea a la cola de progrmacion.

    Args:
        req: request de la funcion, debe tener el parametro llamado "task_name",
            para ejecutar una lista de funciones en especifico.
        msg: mensaje que representa la cola en donde adicionar.
    """
    try:
        req_body = req.get_json()
        task_names = req_body.get('task_name', [])

    except ValueError:
        return func.HttpResponse(
            "La funcion tuvo un error",
            status_code=400,
        )

    if task_names is None:
        return func.HttpResponse("No se enviaron tareas", status_code=400)
        # se ejecuta tarea con el nombre especifico sin importar el archivo de configuracion
    msg.set(task_names)
    return func.HttpResponse(f"tareas programadas: {task_names}", status_code=200)


@framework.queue_trigger(
    arg_name="msg", queue_name=QUEUE_NAME, connection=NAME_CONNECTION_STORAGE_ACCOUNT
)  # Queue trigger
def execute_tasks_inqueue(msg: func.QueueMessage) -> None:
    """Ejecuta tareas de acuerdo al queue."""
    task_name = msg.get_body().decode('utf-8')
    logger.info('execute_tasks_queue va ejecutar la tarea: %s', task_name)
    runner.run_task(task_name)
