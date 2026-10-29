"""Modelos pydantic para el YAML de configuración."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


def _parse_csv_int_or_star(value: Union[str, int, List[Any]]) -> Union[str, List[int]]:
    """Normaliza dias/horas: '*' o lista de enteros."""
    if value == '*' or value == ['*']:
        return '*'
    if isinstance(value, int):
        return [value]
    if isinstance(value, list):
        out: List[int] = []
        for item in value:
            if str(item).strip() == '*':
                return '*'
            out.append(int(str(item).strip()))
        return out
    text = str(value).strip()
    if text == '*':
        return '*'
    return [int(part.strip()) for part in text.split(',') if part.strip()]


class DQSilence(BaseModel):
    """Silencio temporal de la alarma de calidad de datos de una tarea.

    Mientras está vigente (hasta incluido, en la zona horaria de la tarea), un fallo de calidad no lanza
    error ni notifica: solo queda en el log. Al vencer, la alarma vuelve sola.
    """

    model_config = ConfigDict(extra='forbid')

    hasta: date
    motivo: str

    @field_validator('motivo')
    @classmethod
    def _motivo_required(cls, value: str) -> str:
        if not value.strip():
            raise ValueError('motivo no puede estar vacío')
        return value.strip()


class TaskConfig(BaseModel):
    """Configuración de una tarea en el YAML."""

    dias: Union[str, List[int]] = '*'
    horas: Union[str, List[int]] = '*'
    emails: Optional[List[str]] = None
    enabled: bool = True
    cron: Optional[str] = None
    depends_on: List[str] = Field(default_factory=list)
    params: Dict[str, Any] = Field(default_factory=dict)
    timezone: Optional[str] = None
    notify_on_success: Optional[bool] = None
    silenciar_dq: Optional[DQSilence] = None

    @model_validator(mode='before')
    @classmethod
    def _reject_dq_warn_only(cls, data: Any) -> Any:
        if isinstance(data, dict) and 'dq_warn_only' in data:
            raise ValueError("dq_warn_only ya no existe: use silenciar_dq con 'hasta' (fecha) y 'motivo'.")
        return data

    @field_validator('dias', 'horas', mode='before')
    @classmethod
    def _normalize_schedule(cls, value: Any) -> Union[str, List[int]]:
        return _parse_csv_int_or_star(value)

    @field_validator('depends_on', mode='before')
    @classmethod
    def _normalize_depends(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            return [value]
        return list(value)


class FrameworkConfig(BaseModel):
    """Configuración global del framework."""

    emails_notificar: List[str] = Field(default_factory=list)
    tareas: Dict[str, TaskConfig] = Field(default_factory=dict)
    # legado: ignorar si viene en YAML antiguo
    url_logicapp_email: Optional[str] = Field(default=None, exclude=True)

    @model_validator(mode='after')
    def _validate_depends_on(self) -> FrameworkConfig:
        known = set(self.tareas.keys())
        for name, task in self.tareas.items():
            for dep in task.depends_on:
                if dep not in known:
                    raise ValueError(f"Tarea '{name}' depende de '{dep}' que no existe en configuración.")
        _assert_no_cycles(self.tareas)
        return self


def _assert_no_cycles(tareas: Dict[str, TaskConfig]) -> None:
    """Detecta ciclos en depends_on."""
    visiting: Set[str] = set()
    visited: Set[str] = set()

    def dfs(node: str) -> None:
        if node in visiting:
            raise ValueError(f"Ciclo detectado en depends_on involucrando '{node}'.")
        if node in visited:
            return
        visiting.add(node)
        for dep in tareas[node].depends_on:
            dfs(dep)
        visiting.remove(node)
        visited.add(node)

    for name in tareas:
        dfs(name)


def silence_warnings(conf: FrameworkConfig, today: date, days: int = 7) -> List[str]:
    """Avisos de silencios de DQ vencidos o que vencen en los próximos ``days`` días."""
    warnings: List[str] = []
    for name, task in conf.tareas.items():
        silence = task.silenciar_dq
        if silence is None:
            continue
        remaining = (silence.hasta - today).days
        if remaining < 0:
            warnings.append(f"{name}: el silencio de DQ venció el {silence.hasta}; puede quitarse del YAML.")
        elif remaining <= days:
            warnings.append(f"{name}: el silencio de DQ vence el {silence.hasta} ({silence.motivo}).")
    return warnings


def load_framework_config(path: Union[str, Path], *, allow_missing: bool = False) -> FrameworkConfig:
    """Carga y valida el YAML de configuración."""
    path = Path(path)
    if not path.exists():
        if allow_missing:
            return FrameworkConfig()
        raise FileNotFoundError(
            f"No se encontró el archivo de configuración '{path}'. "
            "Defina YAML_CONFIGURACION o cree centraal_dataframework.yaml."
        )
    with path.open('r', encoding='utf-8') as file:
        data = yaml.safe_load(file) or {}
    if not isinstance(data, dict):
        raise ValueError(f"El YAML '{path}' debe ser un objeto/mapping en la raíz.")
    return FrameworkConfig.model_validate(data)
