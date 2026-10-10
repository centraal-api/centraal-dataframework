"""Utils."""

from __future__ import annotations

import json
import uuid
from typing import Any, Dict, Optional, Tuple


def parse_connection_string(connection_string: str) -> Dict[str, str]:
    """Obtiene valores de string de conexion."""
    result: Dict[str, str] = {}
    key_value_pairs = connection_string.split(';')

    for pair in key_value_pairs:
        index_of_equals = pair.find('=')

        if index_of_equals != -1:
            key = pair[:index_of_equals].strip()
            value = pair[index_of_equals + 1 :].strip()
            result[key] = value

    return result


def new_run_id() -> str:
    """Genera un run_id único."""
    return uuid.uuid4().hex


def parse_queue_message(raw: str) -> Tuple[str, Dict[str, Any], str]:
    """Parsea mensaje de cola: texto plano o JSON {task, params, run_id}.

    Returns:
        (task_name, params, run_id)
    """
    text = (raw or '').strip()
    if not text:
        raise ValueError("Mensaje de cola vacío.")
    if text.startswith('{'):
        data = json.loads(text)
        if not isinstance(data, dict):
            raise ValueError("El mensaje JSON debe ser un objeto.")
        task_name = data.get('task') or data.get('task_name')
        if not task_name or not isinstance(task_name, str):
            raise ValueError("El mensaje JSON debe incluir 'task' (string).")
        params = data.get('params') or {}
        if not isinstance(params, dict):
            raise ValueError("'params' debe ser un objeto.")
        run_id = data.get('run_id') or new_run_id()
        return task_name, params, str(run_id)
    return text.strip('"'), {}, new_run_id()


def build_queue_message(task_name: str, params: Optional[Dict[str, Any]] = None, run_id: Optional[str] = None) -> str:
    """Construye mensaje JSON para la cola."""
    payload = {
        'task': task_name,
        'params': params or {},
        'run_id': run_id or new_run_id(),
    }
    return json.dumps(payload, ensure_ascii=False)
