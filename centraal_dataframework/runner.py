"""Runner del framework."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from croniter import croniter

from centraal_dataframework.config import DQSilence, FrameworkConfig, TaskConfig, load_framework_config
from centraal_dataframework.excepciones import (
    ErrorEnTarea,
    ErrorTareaCalidadDatos,
    RecursoNoConfigurado,
    TareaDuplicada,
    TareaNoDefinida,
)
from centraal_dataframework.notifiers import Notifier, build_default_notifier
from centraal_dataframework.registry import check_uses
from centraal_dataframework.resources import DQResult
from centraal_dataframework.settings import Settings, get_settings
from centraal_dataframework.utils import new_run_id

logger = logging.getLogger(__name__)

_runner: Optional['Runner'] = None


class Runner:
    """Clase que representa el runner."""

    def __init__(
        self,
        settings: Optional[Settings] = None,
        conf: Optional[FrameworkConfig] = None,
        notifier: Optional[Notifier] = None,
        history: Any = None,
        allow_missing_config: bool = True,
    ) -> None:
        """Constructor. Puede recibir Settings/Config inyectados (tests)."""
        self.settings = settings or get_settings()
        if conf is not None:
            self.conf = conf
        else:
            self.conf = load_framework_config(self.settings.yaml_path, allow_missing=allow_missing_config)
        self.tasks: Dict[str, Callable] = {}
        self.notifier = notifier or build_default_notifier(self.settings)
        self.history = history
        self._dependents: Dict[str, List[str]] = self._build_dependents()

    def _build_dependents(self) -> Dict[str, List[str]]:
        dependents: Dict[str, List[str]] = {name: [] for name in self.conf.tareas}
        for name, task_conf in self.conf.tareas.items():
            for dep in task_conf.depends_on:
                dependents.setdefault(dep, []).append(name)
        return dependents

    def add_task(self, func: Callable, task_name: str) -> None:
        """Adiciona tareas."""
        in_conf = task_name in self.conf.tareas
        if not in_conf:
            logger.warning(
                "La tarea %s no está en la configuración YAML. Se registra igualmente.",
                task_name,
            )

        already = task_name in self.tasks
        if already:
            if not self.settings.overwrite_task:
                raise TareaDuplicada(task_name)
            logger.warning(
                "Tarea %s sobrescrita. Use SOBREESCRIBIR_TAREA=false para impedir esto.",
                task_name,
            )
        self.tasks[task_name] = func

    def get_task(self, task_name: str) -> Optional[Callable]:
        """Obtiene la tarea."""
        return self.tasks.get(task_name)

    def check_task_resources(self, task_name: str) -> List[str]:
        """Problemas de configuración de los recursos que declara la tarea (sin conectarse)."""
        func = self.get_task(task_name)
        return check_uses(getattr(func, 'uses', ()), self.settings)

    def check_resources(self) -> Dict[str, List[str]]:
        """Problemas de configuración por tarea registrada; solo incluye tareas con problemas."""
        result = {name: self.check_task_resources(name) for name in self.list_tasks()}
        return {name: problems for name, problems in result.items() if problems}

    def list_tasks(self) -> List[str]:
        """Lista nombres de tareas registradas."""
        return sorted(self.tasks.keys())

    def es_programable(self, task_name_conf: TaskConfig | dict, fecha_ejecucion: Optional[datetime] = None) -> bool:
        """Verificar que la tarea puede ser programada."""
        if isinstance(task_name_conf, dict):
            task_name_conf = TaskConfig.model_validate(task_name_conf)

        if not task_name_conf.enabled:
            return False

        if fecha_ejecucion is None:
            fecha_ejecucion = datetime.now(timezone.utc)
        elif fecha_ejecucion.tzinfo is None:
            fecha_ejecucion = fecha_ejecucion.replace(tzinfo=timezone.utc)

        tz_name = task_name_conf.timezone or self.settings.timezone
        local_tz = ZoneInfo(tz_name)
        fecha_local = fecha_ejecucion.astimezone(local_tz)

        if task_name_conf.cron:
            # ¿La expresión cron coincide en este minuto (hora local)?
            from datetime import timedelta

            base = fecha_local.replace(second=0, microsecond=0)
            cron = croniter(task_name_conf.cron, base - timedelta(minutes=1))
            next_fire = cron.get_next(datetime)
            if next_fire.tzinfo is None:
                next_fire = next_fire.replace(tzinfo=local_tz)
            else:
                next_fire = next_fire.astimezone(local_tz)
            return next_fire.replace(second=0, microsecond=0) == base

        allowed_days = task_name_conf.dias
        allowed_hours = task_name_conf.horas

        if allowed_days == '*' and allowed_hours == '*':
            return True

        hora = fecha_local.hour
        dia = fecha_local.weekday()

        day_ok = allowed_days == '*' or dia in allowed_days  # type: ignore[operator]
        hour_ok = allowed_hours == '*' or hora in allowed_hours  # type: ignore[operator]
        return day_ok and hour_ok

    def get_tareas_programables(self, fecha_ejecucion: Optional[datetime] = None) -> List[str]:
        """Obtiene las tareas programables (sin dependencias pendientes)."""
        tareas: List[str] = []
        for task, task_conf in self.conf.tareas.items():
            if task_conf.depends_on:
                # Las dependientes se encolan al terminar el padre; no por timer.
                continue
            if self.es_programable(task_conf, fecha_ejecucion):
                tareas.append(task)
        return tareas

    def next_runs(self, task_name: str, count: int = 5, from_dt: Optional[datetime] = None) -> List[datetime]:
        """Calcula próximas ejecuciones (aprox. para dias/horas; exacto para cron)."""
        task_conf = self.conf.tareas.get(task_name)
        if not task_conf or not task_conf.enabled:
            return []
        from_dt = from_dt or datetime.now(timezone.utc)
        tz = ZoneInfo(task_conf.timezone or self.settings.timezone)
        local = from_dt.astimezone(tz)
        results: List[datetime] = []

        if task_conf.cron:
            cron = croniter(task_conf.cron, local)
            for _ in range(count):
                nxt = cron.get_next(datetime)
                if nxt.tzinfo is None:
                    nxt = nxt.replace(tzinfo=tz)
                results.append(nxt.astimezone(timezone.utc))
            return results

        from datetime import timedelta

        cursor = local.replace(minute=0, second=0, microsecond=0)
        safety = 0
        while len(results) < count and safety < 24 * 60 * 14:
            cursor = cursor + timedelta(hours=1)
            safety += 1
            if self.es_programable(task_conf, cursor.astimezone(timezone.utc)):
                results.append(cursor.astimezone(timezone.utc))
        return results

    def get_emails(self, task_name: Optional[str] = None) -> List[str]:
        """Obtener los emails (por tarea o generales)."""
        if task_name and task_name in self.conf.tareas:
            task_emails = self.conf.tareas[task_name].emails
            if task_emails:
                return list(task_emails)
        return list(self.conf.emails_notificar)

    def get_default_params(self, task_name: str) -> Dict[str, Any]:
        """Params por defecto del YAML."""
        if task_name in self.conf.tareas:
            return dict(self.conf.tareas[task_name].params)
        return {}

    def dependents_of(self, task_name: str) -> List[str]:
        """Tareas que dependen de task_name."""
        return list(self._dependents.get(task_name, []))

    def dq_silence(self, task_name: str, now: Optional[datetime] = None) -> Optional[DQSilence]:
        """Silencio de DQ vigente de la tarea (hasta incluido, en su zona horaria) o None."""
        task_conf = self.conf.tareas.get(task_name)
        if task_conf is None or task_conf.silenciar_dq is None:
            return None
        now = now or datetime.now(timezone.utc)
        today = now.astimezone(ZoneInfo(task_conf.timezone or self.settings.timezone)).date()
        return task_conf.silenciar_dq if today <= task_conf.silenciar_dq.hasta else None

    def _handle_silenced_dq(
        self, task_name: str, run_id: str, started: datetime, error: ErrorTareaCalidadDatos
    ) -> bool:
        """Si hay silencio de DQ vigente, deja el fallo solo en el log (tarea exitosa) y devuelve True."""
        silence = self.dq_silence(task_name)
        if silence is None:
            return False
        logger.warning(
            "DQ silenciada hasta %s (%s): %s expectativas fallaron en %s",
            silence.hasta,
            silence.motivo,
            len(error.failed),
            task_name,
        )
        if self.history:
            self.history.record_success(task_name, run_id, started, error.result)
        return True

    def run_task(
        self,
        task_name: str,
        params: Optional[Dict[str, Any]] = None,
        run_id: Optional[str] = None,
        *,
        notify: bool = True,
        raise_on_error: bool = True,
        notify_dq: Optional[bool] = None,
    ) -> Any:
        """Ejecuta la tarea.

        Un DQResult fallido termina en ErrorTareaCalidadDatos, salvo que la tarea tenga un silencio de DQ
        vigente (``silenciar_dq`` en el YAML): entonces solo se registra en el log y se devuelve el resultado.
        ``notify_dq`` controla la notificación de calidad aparte de ``notify`` (por defecto, igual a ``notify``).
        """
        func_to_execute = self.get_task(task_name)
        emails = self.get_emails(task_name)
        run_id = run_id or new_run_id()
        merged = {**self.get_default_params(task_name), **(params or {})}

        if func_to_execute is None:
            raise TareaNoDefinida(task_name)

        if self.settings.task_must_be_in_conf and task_name not in self.conf.tareas:
            raise TareaNoDefinida(f"{task_name} (no está en configuración YAML)")

        started = datetime.now(timezone.utc)
        if self.history:
            self.history.record_start(task_name, run_id, merged)

        try:
            problems = self.check_task_resources(task_name)
            if problems:
                raise RecursoNoConfigurado(task_name, problems)
            result = func_to_execute(run_id=run_id, params=merged, settings=self.settings)
            _raise_if_dq_failed(task_name, result)
            if self.history:
                self.history.record_success(task_name, run_id, started, result)
            task_conf = self.conf.tareas.get(task_name)
            should_notify_ok = self.settings.notify_on_success or (
                task_conf.notify_on_success if task_conf and task_conf.notify_on_success is not None else False
            )
            if notify and should_notify_ok:
                self.notifier.notify_success(emails, task_name, run_id)
            return result
        except ErrorTareaCalidadDatos as error:
            if self._handle_silenced_dq(task_name, run_id, started, error):
                return error.result
            logger.info("Error de calidad de datos en %s", task_name)
            if self.history:
                self.history.record_failure(task_name, run_id, started, error, url=error.url_docs)
            should_notify_dq = notify if notify_dq is None else notify_dq
            if should_notify_dq:
                self.notifier.notify_dq(emails, error)
            if raise_on_error:
                raise
            return None
        except Exception as error_tarea:  # noqa: BLE001
            logger.error("se presento error en %s", task_name, exc_info=True)
            if self.history:
                self.history.record_failure(task_name, run_id, started, error_tarea)
            if notify:
                self.notifier.notify_error(emails, error_tarea, task_name)
            if raise_on_error:
                raise ErrorEnTarea(task_name) from error_tarea
            return None


def _raise_if_dq_failed(task_name: str, result: Any) -> None:
    """Convierte un DQResult fallido en ErrorTareaCalidadDatos."""
    if isinstance(result, DQResult) and not result.success:
        raise ErrorTareaCalidadDatos(
            message=f"¡Las expectativas fallaron en {task_name}!.",
            url_docs=result.url,
            check_point_result=result.raw,
            failed=result.failed,
            result=result,
        )


def get_runner() -> Runner:
    """Obtiene el Runner singleton (lazy)."""
    global _runner
    if _runner is None:
        settings = get_settings()
        history = None
        if settings.history_enabled:
            from centraal_dataframework.history import RunHistory

            history = RunHistory(settings)
        _runner = Runner(settings=settings, history=history)
    return _runner


def set_runner(runner: Optional[Runner]) -> None:
    """Reemplaza el Runner global (tests)."""
    global _runner
    _runner = runner


def reset_runner() -> None:
    """Resetea el Runner global."""
    set_runner(None)
