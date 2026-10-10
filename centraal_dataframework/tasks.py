"""Decoradores de tareas y TaskContext."""

from __future__ import annotations

import functools
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Union

from centraal_dataframework.excepciones import ErrorTareaCalidadDatos
from centraal_dataframework.resources import DQResult, GreatExpectationsToolKit, get_context, get_datalake
from centraal_dataframework.runner import get_runner
from centraal_dataframework.synapse import SynapseClient, get_synapse
from centraal_dataframework.utils import new_run_id

STR_FMT = 'TAREA: %(task_name)s run=%(run_id)s %(asctime)s-%(levelname)s-%(message)s'
_LOGGER_CONFIGURED: set[str] = set()


@dataclass
class TaskContext:
    """Contexto inyectado a cada tarea. datalake y synapse se crean al primer uso."""

    name: str
    run_id: str
    logger: logging.Logger
    params: Dict[str, Any] = field(default_factory=dict)
    gx: Optional[GreatExpectationsToolKit] = None
    datalake_factory: Callable[[], Any] = field(default=get_datalake, repr=False)
    synapse_factory: Callable[[], SynapseClient] = field(default=get_synapse, repr=False)
    _datalake: Any = field(default=None, init=False, repr=False)
    _synapse: Optional[SynapseClient] = field(default=None, init=False, repr=False)

    @property
    def datalake(self) -> Any:
        """Cliente Datalake (perezoso)."""
        if self._datalake is None:
            self._datalake = self.datalake_factory()
        return self._datalake

    @property
    def synapse(self) -> SynapseClient:
        """Cliente Synapse con la base por defecto (perezoso)."""
        if self._synapse is None:
            self._synapse = self.synapse_factory()
        return self._synapse

    def synapse_for(self, database: str) -> SynapseClient:
        """Cliente Synapse para otra base (p.ej. bronze, silver, gold)."""
        return self.synapse.for_database(database)


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


def _wrap_task(
    func: Callable,
    *,
    task_name: str,
    with_gx: bool = False,
    fail_on_error: bool = True,
) -> Callable:
    """Crea wrapper que construye TaskContext y registra la tarea."""

    @functools.wraps(func)
    def wrapper(*, run_id: Optional[str] = None, params: Optional[Dict[str, Any]] = None, **kwargs):
        run_id = run_id or new_run_id()
        params = params or {}
        base_logger = _get_task_logger(task_name)
        adapter = logging.LoggerAdapter(base_logger, {'task_name': task_name, 'run_id': run_id})

        gx_toolkit = None
        if with_gx:
            gx_toolkit = GreatExpectationsToolKit(get_context(), task_name)

        ctx = TaskContext(
            name=task_name,
            run_id=run_id,
            logger=adapter,  # type: ignore[arg-type]
            params={**params, **kwargs},
            gx=gx_toolkit,
            datalake_factory=get_datalake,
            synapse_factory=get_synapse,
        )
        result = func(ctx)

        if with_gx and fail_on_error and isinstance(result, DQResult) and not result.success:
            raise ErrorTareaCalidadDatos(
                message=f"¡Las expectativas fallaron en {task_name}!.",
                url_docs=result.url,
                check_point_result=result.raw,
                failed=result.failed,
            )
        return result

    get_runner().add_task(wrapper, task_name)
    return wrapper


def task(
    _func: Optional[Callable] = None, *, name: Optional[str] = None
) -> Union[Callable, Callable[[Callable], Callable]]:
    """Decorador para adicionar la tarea.

    Uso:
        @task
        def mi_tarea(ctx: TaskContext): ...

        @task(name="otra")
        def mi_tarea(ctx: TaskContext): ...
    """

    def decorator(func: Callable) -> Callable:
        task_name = name or func.__name__
        return _wrap_task(func, task_name=task_name, with_gx=False)

    if _func is not None:
        return decorator(_func)
    return decorator


def task_dq(
    _func: Optional[Callable] = None,
    *,
    name: Optional[str] = None,
    fail_on_error: bool = True,
) -> Union[Callable, Callable[[Callable], Callable]]:
    """Decorador para tareas de calidad de datos (Great Expectations)."""

    def decorator(func: Callable) -> Callable:
        task_name = name or func.__name__
        return _wrap_task(func, task_name=task_name, with_gx=True, fail_on_error=fail_on_error)

    if _func is not None:
        return decorator(_func)
    return decorator
