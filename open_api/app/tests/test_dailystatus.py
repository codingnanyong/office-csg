def test_get_dailystatus(client, db_session):
    response = client.get("/api/v1.7/dailystatus")
    assert response.status_code == 200