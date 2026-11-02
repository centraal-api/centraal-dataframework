"""Configuración centralizada desde variables de entorno."""

import os
from functools import lru_cache
from typing import Any, Dict, Literal, Optional
from zoneinfo import ZoneInfo

from pydantic import Field, PrivateAttr, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Settings del framework leídas del entorno / App Settings."""

    model_config = SettingsConfigDict(
        env_file='.env',
        env_file_encoding='utf-8',
        extra='ignore',
        case_sensitive=False,
        populate_by_name=True,
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
    synapse_odbc_driver: str = Field(default='ODBC Driver 18 for SQL Server', validation_alias='SYNAPSE_ODBC_DRIVER')
    # Monitoreo de capacidad (capacidad.py): umbrales para recomendar más instancias o una más grande.
    capacidad_cola_max: int = Field(default=5, validation_alias='CAPACIDAD_COLA_MAX')
    capacidad_espera_max_min: float = Field(default=60, validation_alias='CAPACIDAD_ESPERA_MAX_MIN')
    capacidad_memoria_pct: float = Field(default=80, validation_alias='CAPACIDAD_MEMORIA_PCT')

    _instance: Optional[str] = PrivateAttr(default=None)

    @property
    def instance(self) -> Optional[str]:
        """Nombre de la conexión (None = conexión por defecto)."""
        return self._instance

    def env_name(self, variable: str) -> str:
        """Nombre de la variable de entorno para esta conexión (con sufijo si tiene nombre)."""
        return f"{variable}__{self._instance.upper()}" if self._instance else variable

    @field_validator('timezone')
    @classmethod
    def _validate_timezone(cls, value: str) -> str:
        ZoneInfo(value)
        return value

    def get_zoneinfo(self) -> ZoneInfo:
        """Obtiene ZoneInfo configurada."""
        return ZoneInfo(self.timezone)

    def resolve_storage_connection_string(self) -> Optional[str]:
        """Resuelve connection string desde AZURE_STORAGE_CONNECTION_STRING o el app setting nombrado.

        Una conexión con nombre solo usa sus propias variables (incluido STORAGE_CONNECTION_SETTING__<NOMBRE>):
        nunca cae en AzureWebJobsStorage.
        """
        if self.azure_storage_connection_string:
            return self.azure_storage_connection_string
        if self._instance:
            if 'storage_connection_setting' in self.model_fields_set:
                return os.environ.get(self.storage_connection_setting)
            return None
        return os.environ.get(self.storage_connection_setting) or os.environ.get('AzureWebJobsStorage')


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Obtiene Settings cacheadas."""
    return Settings()


# Campos que identifican una conexión. Una conexión con nombre no los hereda de la global (para no
# apuntar en silencio a otro lago o workspace); el resto (timeouts, contenedores, SAS, ...) sí.
CONNECTION_FIELDS = frozenset(
    {
        'azure_storage_connection_string',
        'storage_account_name',
        'storage_connection_setting',
        'synapse_workspace',
        'synapse_server',
        'synapse_database',
        'synapse_user',
        'synapse_password',
    }
)


class _InstanceSettings(Settings):
    """Settings que solo se construyen con los valores explícitos (sin leer el entorno)."""

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, *args, **kwargs):
        """Solo valores pasados al constructor."""
        return (init_settings,)


def _env_with_dotenv() -> Dict[str, str]:
    """Variables del env_file de Settings y del entorno (el entorno tiene prioridad), con claves en mayúscula."""
    values: Dict[str, Any] = {}
    env_file = Settings.model_config.get('env_file')
    if env_file:
        from dotenv import dotenv_values

        encoding = Settings.model_config.get('env_file_encoding')
        files = env_file if isinstance(env_file, (list, tuple)) else [env_file]
        for path in files:
            values.update({k.upper(): v for k, v in dotenv_values(path, encoding=encoding).items() if v is not None})
    values.update({k.upper(): v for k, v in os.environ.items()})
    return values


def _build_instance_settings(instance: str, base: Settings) -> Settings:
    """Construye las Settings de una conexión con nombre a partir de base y las variables con sufijo."""
    suffix = f"__{instance.upper()}"
    env = _env_with_dotenv()
    values: Dict[str, Any] = {}
    for name, field in Settings.model_fields.items():
        alias = field.validation_alias
        if not isinstance(alias, str):
            continue
        if alias.upper() + suffix in env:
            values[alias] = env[alias.upper() + suffix]
        elif name not in CONNECTION_FIELDS and name in base.model_fields_set:
            values[alias] = getattr(base, name)
    settings = _InstanceSettings(**values)
    settings._instance = instance
    return settings


@lru_cache(maxsize=None)
def _cached_instance_settings(instance: str) -> Settings:
    return _build_instance_settings(instance, get_settings())


def settings_for_instance(instance: str, base: Optional[Settings] = None) -> Settings:
    """Settings de una conexión con nombre.

    Los campos de conexión (CONNECTION_FIELDS) salen solo de variables ``<VARIABLE>__<NOMBRE>``; el resto
    se hereda de base (por defecto, las Settings globales) y también se puede sobrescribir con sufijo.
    """
    instance = instance.lower()
    if base is None:
        return _cached_instance_settings(instance)
    return _build_instance_settings(instance, base)


def clear_settings_cache() -> None:
    """Limpia cache de Settings (útil en tests)."""
    get_settings.cache_clear()
    _cached_instance_settings.cache_clear()
