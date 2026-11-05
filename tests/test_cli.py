"""Tests CLI."""

from centraal_dataframework.cli import main
from centraal_dataframework.templates import scaffold


def test_validate_config(yaml_file):
    """validate-config OK."""
    assert main(['validate-config', '-c', str(yaml_file)]) == 0


def test_next_runs(yaml_file):
    """next-runs imprime fechas."""
    assert main(['next-runs', 'alpha', '-c', str(yaml_file), '-n', '2']) == 0


def test_init(tmp_path):
    """Init crea la plantilla."""
    target = tmp_path / 'proj'
    assert main(['init', str(target)]) == 0
    assert (target / 'function_app.py').exists()
    assert (target / 'host.json').exists()
    assert (target / 'infra' / 'main.bicep').exists()


def test_scaffold_force(tmp_path):
    """Scaffold no sobrescribe sin force."""
    scaffold(tmp_path)
    (tmp_path / 'function_app.py').write_text('keep', encoding='utf-8')
    scaffold(tmp_path, force=False)
    assert (tmp_path / 'function_app.py').read_text(encoding='utf-8') == 'keep'


def test_check_resources(yaml_file, tmp_path, monkeypatch, capsys):
    """check-resources falla si una tarea declara un recurso sin configurar."""
    monkeypatch.delenv('SYNAPSE_WORKSPACE', raising=False)
    monkeypatch.delenv('SYNAPSE_SERVER', raising=False)
    (tmp_path / 'mod_check_resources.py').write_text(
        'from centraal_dataframework import task\n\n\n'
        "@task(name='alpha', uses=['synapse'])\n"
        'def alpha(ctx):\n'
        '    """Tarea de prueba."""\n',
        encoding='utf-8',
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    assert main(['check-resources', '-c', str(yaml_file), '-m', 'mod_check_resources']) == 1
    out = capsys.readouterr().out
    assert 'alpha [synapse]: FALTA CONFIGURACIÓN' in out
    assert 'SYNAPSE_WORKSPACE' in out


def test_validate_config_warns_expired_silence(tmp_path, capsys):
    """validate-config avisa un silencio de DQ vencido sin fallar."""
    path = tmp_path / 'cfg.yaml'
    path.write_text(
        'tareas:\n  alpha:\n    silenciar_dq:\n      hasta: 2000-01-01\n      motivo: ticket 1\n', encoding='utf-8'
    )
    assert main(['validate-config', '-c', str(path)]) == 0
    assert 'AVISO: alpha: el silencio de DQ venció el 2000-01-01' in capsys.readouterr().out


def test_scaffold_targets_premium_with_odbc18_python(tmp_path):
    """La plantilla despliega en Premium con Python >= 3.11 (ODBC 18 en la imagen) e identidad administrada."""
    import json

    scaffold(tmp_path)
    bicep = (tmp_path / 'infra' / 'main.bicep').read_text(encoding='utf-8')
    assert "tier: 'ElasticPremium'" in bicep
    assert "param pythonVersion string = '3.12'" in bicep
    assert "@allowed([ '3.11', '3.12' ])" in bicep
    assert "identity: { type: 'SystemAssigned' }" in bicep
    assert "{ name: 'SYNAPSE_ODBC_DRIVER', value: 'ODBC Driver 18 for SQL Server' }" in bicep
    host = json.loads((tmp_path / 'host.json').read_text(encoding='utf-8'))
    assert host['functionTimeout'] == '01:00:00'


def test_scaffold_single_instance_serial_queue(tmp_path):
    """Una instancia siempre encendida, sin elástico ni pre-calentada, y la cola de a un mensaje."""
    import json

    scaffold(tmp_path)
    bicep = (tmp_path / 'infra' / 'main.bicep').read_text(encoding='utf-8')
    assert 'param instancias int = 1' in bicep
    assert 'maximumElasticWorkerCount: instancias' in bicep
    assert 'minimumElasticInstanceCount: instancias' in bicep
    assert 'functionAppScaleLimit: instancias' in bicep
    assert 'preWarmedInstanceCount' not in bicep
    queues = json.loads((tmp_path / 'host.json').read_text(encoding='utf-8'))['extensions']['queues']
    assert queues['batchSize'] == 1 and queues['newBatchThreshold'] == 0


def test_scaffold_yaml_absolute_path_in_azure(tmp_path):
    """En Azure el worker no arranca en la carpeta de la app: el YAML va con ruta absoluta."""
    scaffold(tmp_path)
    bicep = (tmp_path / 'infra' / 'main.bicep').read_text(encoding='utf-8')
    assert "{ name: 'YAML_CONFIGURACION', value: '/home/site/wwwroot/centraal_dataframework.yaml' }" in bicep


def test_scaffold_application_insights(tmp_path):
    """Sin Application Insights no se ven las trazas, el monitor de capacidad ni la prueba de /api/diag."""
    scaffold(tmp_path)
    bicep = (tmp_path / 'infra' / 'main.bicep').read_text(encoding='utf-8')
    assert "resource insights 'Microsoft.Insights/components@" in bicep
    assert "WorkspaceResourceId: logs.id" in bicep
    assert "{ name: 'APPLICATIONINSIGHTS_CONNECTION_STRING', value: insights.properties.ConnectionString }" in bicep


def test_scaffold_synapse_grants_documented(tmp_path):
    """El bicep recuerda todos los permisos que la identidad necesita en Synapse serverless."""
    scaffold(tmp_path)
    bicep = (tmp_path / 'infra' / 'main.bicep').read_text(encoding='utf-8')
    assert 'GRANT VIEW DEFINITION' in bicep
    assert 'GRANT ADMINISTER DATABASE BULK OPERATIONS' in bicep
    assert 'GRANT REFERENCES ON DATABASE SCOPED CREDENTIAL' in bicep


def test_scaffold_requirements_pins_installed_version(tmp_path):
    """El proyecto instala la librería desde PyPI con la versión exacta con la que se generó."""
    from centraal_dataframework import __version__

    scaffold(tmp_path)
    req = (tmp_path / 'requirements.txt').read_text(encoding='utf-8')
    assert f'centraal-dataframework=={__version__}\n' in req
    assert f'# centraal-dataframework[synapse]=={__version__}\n' in req
    assert '>=' not in req.split('azure-functions')[0]
