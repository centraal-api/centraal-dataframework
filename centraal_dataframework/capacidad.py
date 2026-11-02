"""Monitoreo de capacidad: ¿alcanza una instancia que ejecuta las tareas en serie?

La Function arranca con una instancia que procesa un mensaje de la cola a la vez (``host.json``:
``batchSize=1``, ``newBatchThreshold=0``). Este módulo mide dos señales y recomienda qué ajustar:

- **cola acumulada** (mensajes esperando o espera larga): más instancias, que solo ayudan si las tareas
  pendientes son independientes (N instancias = N tareas a la vez);
- **memoria alta**: instancia más grande (EP2/EP3); más instancias no le dan más memoria a una tarea.
"""

from __future__ import annotations

import logging
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, List, Optional, Tuple

from centraal_dataframework.settings import Settings

logger = logging.getLogger(__name__)

_CGROUP_FILES = ('/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes')
_SIN_LIMITE_MB = 1024 * 1024  # cgroup v1 reporta un número enorme cuando no hay límite


@dataclass
class Medicion:
    """Señales de capacidad en un momento dado (None = no disponible)."""

    mensajes_en_cola: Optional[int]
    espera_max_min: Optional[float]
    memoria_pico_mb: Optional[float]
    memoria_limite_mb: Optional[float]

    def as_dict(self) -> dict:
        """Diccionario para el log."""
        return asdict(self)


def memoria_pico_mb() -> Optional[float]:
    """Pico de memoria del proceso worker desde que arrancó (Linux); None si no se puede medir."""
    try:
        import resource
    except ImportError:  # Windows
        return None
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024  # KB en Linux


def memoria_limite_mb() -> Optional[float]:
    """Memoria de la instancia según el cgroup; None si no hay límite o no se puede leer."""
    for path in _CGROUP_FILES:
        try:
            raw = open(path, encoding='utf-8').read().strip()
        except OSError:
            continue
        if raw == 'max':
            return None
        try:
            mb = int(raw) / (1024 * 1024)
        except ValueError:
            continue
        return None if mb >= _SIN_LIMITE_MB else mb
    return None


def _ahora(ahora: Optional[datetime]) -> datetime:
    return ahora or datetime.now(timezone.utc)


def espera_de_mensaje(msg: Any, ahora: Optional[datetime] = None) -> Optional[float]:
    """Minutos que un mensaje esperó en la cola antes de ejecutarse."""
    inserted = getattr(msg, 'insertion_time', None)
    if not inserted:
        return None
    return (_ahora(ahora) - inserted).total_seconds() / 60


def cliente_cola(settings: Settings, queue_name: str, connection: str) -> Any:
    """Cliente de la cola de tareas, con connection string o identidad (``<conexión>__accountName``)."""
    from azure.storage.queue import QueueClient

    conn_str = os.environ.get(connection)
    if conn_str:
        return QueueClient.from_connection_string(conn_str, queue_name)
    account = os.environ.get(f'{connection}__accountName')
    if not account:
        raise RuntimeError(f"Sin {connection} ni {connection}__accountName para leer la cola")
    from azure.identity import DefaultAzureCredential

    return QueueClient(f'https://{account}.queue.core.windows.net', queue_name, credential=DefaultAzureCredential())


def estado_cola(cliente: Any, ahora: Optional[datetime] = None) -> Tuple[int, float]:
    """Mensajes en la cola (aproximado) y minutos de espera del mensaje visible más viejo."""
    count = cliente.get_queue_properties().approximate_message_count or 0
    mensajes = list(cliente.peek_messages(max_messages=1))
    espera = (_ahora(ahora) - mensajes[0].inserted_on).total_seconds() / 60 if mensajes else 0.0
    return count, espera


def medir(settings: Settings, queue_name: str = 'tareas', connection: str = 'AzureWebJobsStorage') -> Medicion:
    """Toma las señales; lo que no se puede medir queda en None."""
    count: Optional[int] = None
    espera: Optional[float] = None
    try:
        count, espera = estado_cola(cliente_cola(settings, queue_name, connection))
    except Exception:  # noqa: BLE001
        logger.warning("No se pudo leer la cola %s para el monitoreo de capacidad", queue_name, exc_info=True)
    return Medicion(count, espera, memoria_pico_mb(), memoria_limite_mb())


def evaluar(m: Medicion, settings: Settings) -> List[str]:
    """Recomendaciones (vacía si una instancia en serie alcanza)."""
    recs: List[str] = []
    cola_alta = m.mensajes_en_cola is not None and m.mensajes_en_cola > settings.capacidad_cola_max
    espera_alta = m.espera_max_min is not None and m.espera_max_min > settings.capacidad_espera_max_min
    if cola_alta or espera_alta:
        recs.append(
            f"Cola acumulada ({m.mensajes_en_cola} mensajes, el más viejo espera {m.espera_max_min or 0:.0f} min). "
            "Considere otra instancia (maximumInstances/minimumInstances): N instancias ejecutan N tareas "
            "independientes a la vez; las dependientes siguen en serie."
        )
    if m.memoria_pico_mb and m.memoria_limite_mb:
        pct = 100 * m.memoria_pico_mb / m.memoria_limite_mb
        if pct > settings.capacidad_memoria_pct:
            recs.append(
                f"Memoria pico {m.memoria_pico_mb:.0f} MB ({pct:.0f}% de {m.memoria_limite_mb:.0f} MB). "
                "Considere una instancia más grande (EP2/EP3) o mover ese cómputo al motor; "
                "más instancias no dan más memoria a una tarea."
            )
    return recs
