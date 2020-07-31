def test_healthcheck_view(app_with_db):
    client = app_with_db.test_client()
    response = client.get('/healthcheck/')
    assert response.status_code == 200
    assert response.json == {'status': 'OK'}
