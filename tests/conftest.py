"""Fixtures compartidas."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from centraal_dataframework.config import FrameworkConfig, TaskConfig
from centraal_dataframework.notifiers import LogicAppNotifier
from centraal_dataframework.runner import Runner, reset_runner, set_runner
from centraal_dataframework.settings import Settings, clear_settings_cache


@pytest.fixture(autouse=True)
def _clean_runner_and_settings(monkeypatch, tmp_path):
    """Aísla Settings/Runner entre tests."""
    clear_settings_cache()
    reset_runner()
    monkeypatch.setenv('YAML_CONFIGURACION', str(tmp_path / 'missing.yaml'))
    monkeypatch.delenv('LOGICAPP_EMAIL_URL', raising=False)
    monkeypatch.delenv('AZURE_STORAGE_CONNECTION_STRING', raising=False)
    yield
    reset_runner()
    clear_settings_cache()


@pytest.fixture
def sample_conf() -> FrameworkConfig:
    """Config de ejemplo."""
    return FrameworkConfig(
        emails_notificar=['ops@example.com'],
        tareas={
            'alpha': TaskConfig(dias='*', horas='8,12', emails=['alpha@example.com']),
            'beta': TaskConfig(dias='0,2', horas='*'),
            'gamma': TaskConfig(dias='*', horas='*', depends_on=['alpha']),
            'disabled': TaskConfig(dias='*', horas='*', enabled=False),
            'cron_task': TaskConfig(cron='0 8 * * 1-5', timezone='America/Bogota'),
        },
    )


@pytest.fixture
def settings(tmp_path) -> Settings:
    """Settings de test."""
    clear_settings_cache()
    return Settings(
        yaml_path=str(tmp_path / 'cfg.yaml'),
        logicapp_email_url='https://example.com/logic',
        timezone='America/Bogota',
        overwrite_task=True,
        task_must_be_in_conf=False,
        max_retries=5,
        history_enabled=False,
    )


@pytest.fixture
def runner(settings, sample_conf) -> Runner:
    """Runner inyectado como global."""
    r = Runner(settings=settings, conf=sample_conf, notifier=None)
    r.notifier = LogicAppNotifier(settings.logicapp_email_url)
    set_runner(r)
    return r


@pytest.fixture
def yaml_file(tmp_path, sample_conf) -> Path:
    """Escribe YAML válido."""
    path = tmp_path / 'centraal_dataframework.yaml'
    data = {
        'emails_notificar': sample_conf.emails_notificar,
        'tareas': {k: v.model_dump() for k, v in sample_conf.tareas.items()},
    }
    for task in data['tareas'].values():
        if isinstance(task['dias'], list):
            task['dias'] = ','.join(str(x) for x in task['dias'])
        if isinstance(task['horas'], list):
            task['horas'] = ','.join(str(x) for x in task['horas'])
    path.write_text(yaml.safe_dump(data), encoding='utf-8')
    return path
