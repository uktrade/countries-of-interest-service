from unittest import mock

from app.commands.algorithm import interaction_coi_extraction, standardise_countries


class TestAlgorithmCommand:
    def test_standardise_countries_cmd_help(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(standardise_countries, ['--help'])
        assert 'Usage: standardise_countries [OPTIONS]' in result.output
        assert result.exit_code == 0
        assert result.exception is None

    def test_interaction_coi_extraction_cmd_help(self, app_with_db):
        runner = app_with_db.test_cli_runner()
        result = runner.invoke(interaction_coi_extraction, ['--help'])
        assert 'Usage: interaction_coi_extraction [OPTIONS]' in result.output
        assert result.exit_code == 0
        assert result.exception is None

    @mock.patch('app.commands.algorithm.standardise')
    def test_run_standardise_countries(self, mock_standardise_countries, app_with_db):
        mock_standardise_countries.return_value = None
        runner = app_with_db.test_cli_runner()
        runner.invoke(standardise_countries)
        assert mock_standardise_countries.called is True

    @mock.patch('app.commands.algorithm.analyse_interactions')
    def test_run_analyse_interactions(self, mock_analyse_interactions, app_with_db):
        mock_analyse_interactions.return_value = None
        runner = app_with_db.test_cli_runner()
        runner.invoke(interaction_coi_extraction)
        assert mock_analyse_interactions.called is True
