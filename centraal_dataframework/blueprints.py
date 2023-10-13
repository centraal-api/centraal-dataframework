"""Modulo de blueprints."""
import logging

import azure.functions as func
from centraal_dataframework.runner import Runner
from centraal_dataframework.excepciones import ErrorEnTarea
from centraal_dataframework.excepciones import TareaNoDefinida


logger = logging.getLogger(__name__)
NAME_CONNECTION_STORAGE_ACCOUNT = "MyStorageAccountAppSetting"
QUEUE_NAME = "tareas"
runner = Runner()
framework = func.Blueprint()


@framework.route(methods=['post'])
@framework.queue_output(arg_name="msg", queue_name=QUEUE_NAME, connection=NAME_CONNECTION_STORAGE_ACCOUNT)
def queue_task(req: func.HttpRequest, msg: func.Out[str]) -> func.HttpResponse:
    """Lee el archivo yml y define que tareas ejecutar.

    Tambien puede usar para programar una lista de funciones.

    Args:
        req: request de la funcion, puede tener un body vacio o tener un parametro
            "task_name", para ejecutar una lista de funciones en especifico.
        msg: mensaje que representa

    """
    logger.info('ejecutando la funcion run_tasks')
    logger.info('leyendo el archivo .yaml de configuración')
    # TODO: leer el archivo (quizas usar pydantic?)
    try:
        req_body = req.get_json()
        task_names = req_body.get('task_name', [])

    except ValueError:
        return func.HttpResponse(
            "La funcion tuvo un error",
            status_code=400,
        )

    if task_names is not None:
        # se ejecuta tarea con el nombre especifico sin importar el archivo de configuracion
        msg.set(task_names)
    else:
        # de acuerdo al retorno del archivo yml, solo se encolan el listado

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

    if func_to_execute is None:
        raise TareaNoDefinida(task_name)
    try:
        func_to_execute()
    except Exception as error_tarea:
        logger.error("se presento error en %s", task_name, exc_info=True)
        # TODO: enviar correo
        raise ErrorEnTarea(task_name) from error_tarea
