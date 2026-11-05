"""Tests de Settings."""

from centraal_dataframework.settings import Settings, clear_settings_cache, get_settings


def test_bool_env_false(monkeypatch):
    """SOBREESCRIBIR_TAREA=false debe ser bool False."""
    monkeypatch.setenv('SOBREESCRIBIR_TAREA', 'false')
    monkeypatch.setenv('TAREA_DEBE_CONFIGURACION', '0')
    clear_settings_cache()
    s = get_settings()
    assert s.overwrite_task is False
    assert s.task_must_be_in_conf is False


def test_timezone_default():
    """Timezone por defecto America/Bogota."""
    s = Settings()
    assert s.timezone == 'America/Bogota'
    assert s.get_zoneinfo().key == 'America/Bogota'


def test_utc_offset_no_longer_breaks_import(monkeypatch):
    """UTC_OFFSET legado no debe romper import; se usa TIMEZONE."""
    monkeypatch.setenv('UTC_OFFSET', '-5')
    clear_settings_cache()
    s = get_settings()
    assert s.timezone == 'America/Bogota'


def test_resolve_storage_account_name_desde_sufijo_accountname(monkeypatch):
    """<conexión>__accountName (conexión por identidad del host) resuelve la cuenta sin STORAGE_ACCOUNT_NAME."""
    monkeypatch.setenv('AzureWebJobsStorage__accountName', 'stcuenta')
    s = Settings(_env_file=None, storage_account_name=None)
    assert s.resolve_storage_account_name() == 'stcuenta'


def test_resolve_storage_account_name_prefiere_storage_account_name(monkeypatch):
    """STORAGE_ACCOUNT_NAME gana sobre el sufijo de la conexión."""
    monkeypatch.setenv('AzureWebJobsStorage__accountName', 'stcuenta')
    s = Settings(_env_file=None, storage_account_name='stotra')
    assert s.resolve_storage_account_name() == 'stotra'
