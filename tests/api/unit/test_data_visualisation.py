import unittest
import unittest.mock

import app.api.views as views


def test_template(app):
    with unittest.mock.patch('app.api.views.render_template') as mock_render_template:
        with unittest.mock.patch(
            'app.sso.token.is_authenticated'
        ) as mock_is_authenticated:
            mock_is_authenticated.return_value = True
            with app.test_request_context():
                response = views.data_visualisation()
                mock_render_template.assert_called_once_with('data-visualisation.html')
                assert response == mock_render_template.return_value
