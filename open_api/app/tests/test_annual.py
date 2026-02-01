def test_get_annual(client, db_session):
    response = client.get("/api/v1.7/annual")
    assert response.status_code == 200

def test_get_annual_last(client, db_session):
    response = client.get("/api/v1.7/annual/last")
    assert response.status_code == 200

def test_get_annual_latest(client, db_session):
    response = client.get("/api/v1.7/annual/latest")
    assert response.status_code == 200
