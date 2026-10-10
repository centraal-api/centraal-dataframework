"""Configuración centralizada desde variables de entorno."""

from functools import lru_cache
from typing import Literal, Optional
from zoneinfo import ZoneInfo

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings del framework leídas del entorno / App Settings."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
        case_sensitive=False,
    )

    storage_connection_setting: str = Field(
        default='AzureWebJobsStorage',
        validation_alias='STORAGE_CONNECTION_SETTING',
    )
    azure_storage_connection_string: Optional[str] = Field(
        default=None,
        validation_alias='AZURE_STORAGE_CONNECTION_STRING',
    )
    storage_account_name: Optional[str] = Field(default=None, validation_alias='STORAGE_ACCOUNT_NAME')
    queue_name: str = Field(default='tareas', validation_alias='QUEUE_NAME')
    yaml_path: str = Field(default='centraal_dataframework.yaml', validation_alias='YAML_CONFIGURACION')
    timezone: str = Field(default='America/Bogota', validation_alias='TIMEZONE')
    overwrite_task: bool = Field(default=True, validation_alias='SOBREESCRIBIR_TAREA')
    task_must_be_in_conf: bool = Field(default=True, validation_alias='TAREA_DEBE_CONFIGURACION')
    logicapp_email_url: Optional[str] = Field(default=None, validation_alias='LOGICAPP_EMAIL_URL')
    validations_container: str = Field(default='calidad-datos', validation_alias='CONTENEDOR_VALIDACIONES')
    docs_public_base_url: Optional[str] = Field(default=None, validation_alias='DOCS_PUBLIC_BASE_URL')
    sas_duration: int = Field(default=7, validation_alias='SAS_VALOR_EXPIRACION')
    sas_unit: str = Field(default='day', validation_alias='SAS_UNIDAD_EXPIRACION')
    sas_ip: Optional[str] = Field(default=None, validation_alias='SAS_IP')
    max_retries: int = Field(default=5, validation_alias='MAX_RETRIES')
    history_table_name: str = Field(default='taskruns', validation_alias='HISTORY_TABLE_NAME')
    history_enabled: bool = Field(default=False, validation_alias='HISTORY_ENABLED')
    teams_webhook_url: Optional[str] = Field(default=None, validation_alias='TEAMS_WEBHOOK_URL')
    slack_webhook_url: Optional[str] = Field(default=None, validation_alias='SLACK_WEBHOOK_URL')
    notify_on_success: bool = Field(default=False, validation_alias='NOTIFY_ON_SUCCESS')
    synapse_workspace: Optional[str] = Field(default=None, validation_alias='SYNAPSE_WORKSPACE')
    synapse_server: Optional[str] = Field(default=None, validation_alias='SYNAPSE_SERVER')
    synapse_database: Optional[str] = Field(default=None, validation_alias='SYNAPSE_DATABASE')
    synapse_auth: Literal['identity', 'sql'] = Field(default='identity', validation_alias='SYNAPSE_AUTH')
    synapse_user: Optional[str] = Field(default=None, validation_alias='SYNAPSE_USER')
    synapse_password: Optional[SecretStr] = Field(default=None, validation_alias='SYNAPSE_PASSWORD')
    synapse_query_timeout: int = Field(default=1800, validation_alias='SYNAPSE_QUERY_TIMEOUT')

    @field_validator('timezone')
    @classmethod
    def _validate_timezone(cls, value: str) -> str:
        ZoneInfo(value)
        return value

    def get_zoneinfo(self) -> ZoneInfo:
        """Obtiene ZoneInfo configurada."""
        return ZoneInfo(self.timezone)

    def resolve_storage_connection_string(self) -> Optional[str]:
        """Resuelve connection string desde AZURE_STORAGE_CONNECTION_STRING o el app setting nombrado."""
        if self.azure_storage_connection_string:
            return self.azure_storage_connection_string
        import os

        return os.environ.get(self.storage_connection_setting) or os.environ.get('AzureWebJobsStorage')


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Obtiene Settings cacheadas."""
    return Settings()


def clear_settings_cache() -> None:
    """Limpia cache de Settings (útil en tests)."""
    get_settings.cache_clear()
