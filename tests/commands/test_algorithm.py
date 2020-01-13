from unittest import mock

from app.commands.algorithm import AlgorithmCommand


class TestAlgorithmCommand:
    def test_cmd_config(self, app_with_db):
        AlgorithmCommand(app_with_db)
        assert list(AlgorithmCommand._commands.keys()) == [
            'standardise_countries',
            'interaction_coi_extraction',
        ]

        cmd = AlgorithmCommand._commands['standardise_countries']
        options = self.get_option_names_from_cmd(cmd)
        assert set(options) == set({})

        cmd = AlgorithmCommand._commands['interaction_coi_extraction']
        options = self.get_option_names_from_cmd(cmd)
        assert set(options) == set({})

    def get_option_names_from_cmd(self, cmd):
        options = []
        for option in cmd.option_list:
            options.append(option.args[0])
        return options

    @mock.patch('app.commands.algorithm.standardise')
    def test_run_standardise_countries(self, mock_standardise_countries, app_with_db):
        mock_standardise_countries.return_value = None
        cmd = AlgorithmCommand._commands['standardise_countries']
        cmd.run()
        assert mock_standardise_countries.called is True

    @mock.patch('app.commands.algorithm.analyse_interactions')
    def test_run_analyse_interactions(self, mock_analyse_interactions, app_with_db):
        mock_analyse_interactions.return_value = None
        cmd = AlgorithmCommand._commands['interaction_coi_extraction']
        cmd.run()
        assert mock_analyse_interactions.called is True
