"""Blueprints de Azure Functions."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import List, Union

import azure.functions as func

from centraal_dataframework.excepciones import ErrorEnTarea, TareaNoDefinida
from centraal_dataframework.runner import get_runner
from centraal_dataframework.settings import get_settings
from centraal_dataframework.utils import build_queue_message, parse_queue_message

logger = logging.getLogger(__name__)

QUEUE_NAME = os.environ.get('QUEUE_NAME', 'tareas')
CONNECTION = os.environ.get('STORAGE_CONNECTION_SETTING', 'AzureWebJobsStorage')
framework = func.Blueprint()


@framework.schedule(schedule="0 0 * * * *", arg_name="mytimer", run_on_startup=False)
@framework.queue_output(arg_name="msg", queue_name=QUEUE_NAME, connection=CONNECTION)
def check_and_schedule_task(mytimer: func.TimerRequest, msg: func.Out[List[str]]):
    """Verifica que tareas se deben programar y las encola."""
    utc_timestamp = datetime.now(timezone.utc)

    if mytimer.past_due:
        logger.warning('La función se ejecutó fuera de tiempo')

    runner = get_runner()
    tareas = runner.get_tareas_programables(utc_timestamp)
    if not tareas:
        logger.warning("No se programaron tareas")
        return

    messages = [build_queue_message(t) for t in tareas]
    logger.info("se programan %s", tareas)
    msg.set(messages)


@framework.route(methods=['post'], auth_level=func.AuthLevel.FUNCTION)
@framework.queue_output(arg_name="msg", queue_name=QUEUE_NAME, connection=CONNECTION)
def queue_task(req: func.HttpRequest, msg: func.Out[Union[str, List[str]]]) -> func.HttpResponse:
    """Adiciona una o más tareas a la cola."""
    runner = get_runner()
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "JSON inválido"}),
            status_code=400,
            mimetype='application/json',
        )

    if not isinstance(req_body, dict):
        return func.HttpResponse(
            json.dumps({"error": "El body debe ser un objeto JSON"}),
            status_code=400,
            mimetype='application/json',
        )

    raw_names = req_body.get('task_name', req_body.get('task'))
    params = req_body.get('params') or {}
    if not isinstance(params, dict):
        return func.HttpResponse(
            json.dumps({"error": "'params' debe ser un objeto"}),
            status_code=400,
            mimetype='application/json',
        )

    if raw_names is None or raw_names == [] or raw_names == '':
        return func.HttpResponse(
            json.dumps({"error": "No se enviaron tareas", "available": runner.list_tasks()}),
            status_code=400,
            mimetype='application/json',
        )

    if isinstance(raw_names, str):
        task_names = [raw_names]
    elif isinstance(raw_names, list):
        task_names = [str(n) for n in raw_names if n]
    else:
        return func.HttpResponse(
            json.dumps({"error": "task_name debe ser string o lista"}),
            status_code=400,
            mimetype='application/json',
        )

    if not task_names:
        return func.HttpResponse(
            json.dumps({"error": "No se enviaron tareas", "available": runner.list_tasks()}),
            status_code=400,
            mimetype='application/json',
        )

    missing = [t for t in task_names if runner.get_task(t) is None]
    if missing:
        return func.HttpResponse(
            json.dumps({"error": "Tareas no definidas", "missing": missing, "available": runner.list_tasks()}),
            status_code=404,
            mimetype='application/json',
        )

    messages = [build_queue_message(t, params=params) for t in task_names]
    if len(messages) == 1:
        msg.set(messages[0])
    else:
        msg.set(messages)

    return func.HttpResponse(
        json.dumps({"scheduled": task_names}),
        status_code=200,
        mimetype='application/json',
    )


@framework.route(methods=['get'], route="status/{run_id?}", auth_level=func.AuthLevel.FUNCTION)
def task_status(req: func.HttpRequest) -> func.HttpResponse:
    """Consulta estado de ejecución (historial Table Storage)."""
    runner = get_runner()
    run_id = req.route_params.get('run_id') or req.params.get('run_id')
    task_name = req.params.get('task')
    if not runner.history:
        return func.HttpResponse(
            json.dumps({"error": "Historial no habilitado (HISTORY_ENABLED=true)"}),
            status_code=501,
            mimetype='application/json',
        )
    if run_id:
        row = runner.history.get_run(run_id)
        if not row:
            return func.HttpResponse(
                json.dumps({"error": "No encontrado"}), status_code=404, mimetype='application/json'
            )
        return func.HttpResponse(json.dumps(row, default=str), status_code=200, mimetype='application/json')
    rows = runner.history.list_runs(task_name=task_name, limit=50)
    return func.HttpResponse(json.dumps(rows, default=str), status_code=200, mimetype='application/json')


@framework.queue_trigger(arg_name="msg", queue_name=QUEUE_NAME, connection=CONNECTION)
def execute_tasks_inqueue(msg: func.QueueMessage) -> None:
    """Ejecuta tareas de acuerdo al queue."""
    runner = get_runner()
    settings = get_settings()
    raw = msg.get_body().decode('utf-8')
    try:
        task_name, params, run_id = parse_queue_message(raw)
    except (ValueError, json.JSONDecodeError) as exc:
        logger.error("Mensaje de cola inválido: %s", exc)
        return

    logger.info('execute_tasks_queue va ejecutar la tarea: %s run_id=%s', task_name, run_id)

    if runner.get_task(task_name) is None:
        logger.error("Tarea no definida: %s — no se reintentará", task_name)
        return

    dequeue_count = getattr(msg, 'dequeue_count', 1) or 1
    max_retries = settings.max_retries
    is_last_attempt = dequeue_count >= max_retries

    try:
        runner.run_task(task_name, params=params, run_id=run_id, notify=is_last_attempt, raise_on_error=True)
    except TareaNoDefinida:
        logger.error("Tarea no definida en ejecución: %s", task_name)
        return
    except ErrorEnTarea:
        if is_last_attempt:
            logger.error("Tarea %s falló en el último intento (dequeue_count=%s)", task_name, dequeue_count)
        else:
            logger.warning(
                "Tarea %s falló (intento %s/%s); se reintentará",
                task_name,
                dequeue_count,
                max_retries,
            )
        raise
    else:
        for dep in runner.dependents_of(task_name):
            logger.info("Ejecutando dependiente %s de %s", dep, task_name)
            try:
                runner.run_task(dep, params=params, notify=True, raise_on_error=True)
            except ErrorEnTarea:
                logger.exception("Falló dependiente %s", dep)
