"""Decoradores de tareas y TaskContext."""

from __future__ import annotations

import functools
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterable, Optional, Tuple, Union

from centraal_dataframework.excepciones import RecursoNoDeclarado
from centraal_dataframework.registry import format_use, get_resource, normalize_uses, parse_use, settings_for
from centraal_dataframework.resources import GreatExpectationsToolKit
from centraal_dataframework.runner import get_runner
from centraal_dataframework.settings import Settings
from centraal_dataframework.synapse import SynapseClient
from centraal_dataframework.utils import new_run_id

STR_FMT = 'TAREA: %(task_name)s run=%(run_id)s %(asctime)s-%(levelname)s-%(message)s'
_LOGGER_CONFIGURED: set[str] = set()


@dataclass
class TaskContext:
    """Contexto inyectado a cada tarea.

    Solo expone los recursos declarados en ``@task(uses=[...])``. Cada recurso se crea al primer uso;
    usar uno no declarado lanza RecursoNoDeclarado.
    """

    name: str
    run_id: str
    logger: logging.Logger
    params: Dict[str, Any] = field(default_factory=dict)
    uses: Tuple[str, ...] = ()
    settings: Optional[Settings] = field(default=None, repr=False)
    resources: Dict[str, Any] = field(default_factory=dict, repr=False)

    def resource(self, name: str, instance: Optional[str] = None) -> Any:
        """Devuelve un recurso declarado, creándolo al primer uso.

        Acepta la conexión por defecto (``'datalake'``) o una con nombre (``'datalake', 'curado'`` o
        ``'datalake:curado'``).
        """
        name, instance = parse_use(name if instance is None else f"{name}:{instance}")
        key = format_use(name, instance)
        if key not in self.uses:
            raise RecursoNoDeclarado(self.name, key, self.uses)
        if key not in self.resources:
            factory = get_resource(name).factory
            self.resources[key] = factory(settings_for(instance, self.settings), self)
        return self.resources[key]

    @property
    def datalake(self) -> Any:
        """Datalake por defecto (requiere ``uses=['datalake']``)."""
        return self.resource('datalake')

    @property
    def synapse(self) -> SynapseClient:
        """Synapse por defecto con su base por defecto (requiere ``uses=['synapse']``)."""
        return self.resource('synapse')

    def synapse_for(self, database: str) -> SynapseClient:
        """Synapse por defecto apuntando a otra base (p.ej. bronze, silver, gold)."""
        return self.synapse.for_database(database)

    @property
    def gx(self) -> GreatExpectationsToolKit:
        """Toolkit de Great Expectations (requiere ``uses=['gx']``)."""
        return self.resource('gx')


def _get_task_logger(task_name: str) -> logging.Logger:
    """Logger por tarea sin handlers duplicados."""
    logger = logging.getLogger(f"centraal_dataframework.task.{task_name}")
    if task_name not in _LOGGER_CONFIGURED:
        if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(STR_FMT))
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        _LOGGER_CONFIGURED.add(task_name)
    return logger


def _wrap_task(func: Callable, *, task_name: str, uses: Tuple[str, ...]) -> Callable:
    """Crea wrapper que construye TaskContext y registra la tarea."""

    @functools.wraps(func)
    def wrapper(
        *,
        run_id: Optional[str] = None,
        params: Optional[Dict[str, Any]] = None,
        settings: Optional[Settings] = None,
        **kwargs,
    ):
        run_id = run_id or new_run_id()
        params = params or {}
        base_logger = _get_task_logger(task_name)
        adapter = logging.LoggerAdapter(base_logger, {'task_name': task_name, 'run_id': run_id})

        ctx = TaskContext(
            name=task_name,
            run_id=run_id,
            logger=adapter,  # type: ignore[arg-type]
            params={**params, **kwargs},
            uses=uses,
            settings=settings,
        )
        return func(ctx)

    wrapper.uses = uses  # type: ignore[attr-defined]
    get_runner().add_task(wrapper, task_name)
    return wrapper


def task(
    _func: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    uses: Iterable[str] = (),
) -> Union[Callable, Callable[[Callable], Callable]]:
    """Decorador para registrar una tarea.

    ``uses`` declara las plataformas que la tarea carga en su contexto; cada una debe estar registrada
    (ver ``register_resource``). Si la tarea devuelve un DQResult fallido, el runner lo convierte en
    ErrorTareaCalidadDatos (salvo silencio de DQ en el YAML); en una llamada directa se devuelve tal cual.

    Uso:
        @task
        def mi_tarea(ctx: TaskContext): ...

        @task(name="otra", uses=['synapse', 'datalake:curado', 'gx'])
        def mi_tarea(ctx: TaskContext): ...
    """
    declared = normalize_uses(uses)

    def decorator(func: Callable) -> Callable:
        return _wrap_task(func, task_name=name or func.__name__, uses=declared)

    if _func is not None:
        return decorator(_func)
    return decorator
