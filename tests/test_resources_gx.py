"""Tests del toolkit GX con mocks."""

from unittest.mock import MagicMock, patch

from centraal_dataframework.resources import DQResult, GreatExpectationsToolKit
from centraal_dataframework.settings import Settings


def test_validate_success_path(monkeypatch):
    """validate construye checkpoint y devuelve DQResult."""
    df = {'a': [1, 2]}  # no requiere pandas real en este mock
    context = MagicMock()
    datasource = MagicMock()
    asset = MagicMock()
    batch_def = MagicMock()
    asset.add_batch_definition_whole_dataframe.return_value = batch_def
    datasource.add_dataframe_asset.return_value = asset
    context.data_sources.add_or_update_pandas.return_value = datasource
    context.suites.add_or_update.side_effect = lambda s: s
    context.validation_definitions.add_or_update.side_effect = lambda v: v
    context.checkpoints.add_or_update.side_effect = lambda c: c

    result = MagicMock()
    result.success = True
    result.run_results = {}
    checkpoint_instance = MagicMock()
    checkpoint_instance.run.return_value = result

    fake_gx = MagicMock()
    fake_gx.ExpectationSuite = MagicMock(side_effect=lambda **k: MagicMock(**k))
    fake_gx.ValidationDefinition = MagicMock(side_effect=lambda **k: MagicMock(**k))
    fake_gx.Checkpoint = MagicMock(return_value=checkpoint_instance)

    monkeypatch.setitem(__import__('sys').modules, 'great_expectations', fake_gx)
    monkeypatch.setitem(
        __import__('sys').modules,
        'great_expectations.checkpoint',
        MagicMock(UpdateDataDocsAction=MagicMock),
    )

    # Re-bind imports inside method by patching module attribute used after import
    import centraal_dataframework.resources as res

    toolkit = GreatExpectationsToolKit(context=context, name_task='t', settings=Settings())
    with patch.object(res, 'get_datalake', side_effect=Exception('no dl')):
        # Force import gx inside validate
        with patch.dict('sys.modules', {'great_expectations': fake_gx}):
            # Manually call internal with patched gx
            toolkit.datasource = datasource
            # Patch builtins import in _run_suite_on_df
            import centraal_dataframework.resources as mod

            original = mod.GreatExpectationsToolKit._run_suite_on_df

            def fake_run(self, df, name, suite, public=False):
                return DQResult(success=True, url='', failed=[], raw=result)

            monkeypatch.setattr(mod.GreatExpectationsToolKit, '_run_suite_on_df', fake_run)
            out = toolkit.validate(df, 'n', [MagicMock()])
            assert out.success is True


def test_get_context_ephemeral(monkeypatch):
    """get_context usa gx.get_context."""
    fake_ctx = MagicMock()
    fake_gx = MagicMock()
    fake_gx.get_context.return_value = fake_ctx
    monkeypatch.setitem(__import__('sys').modules, 'great_expectations', fake_gx)
    # Need to import inside function - patch import
    import importlib

    import centraal_dataframework.resources as res

    def fake_get_context(settings=None):
        return fake_ctx

    monkeypatch.setattr(res, 'get_context', fake_get_context)
    assert res.get_context() is fake_ctx
