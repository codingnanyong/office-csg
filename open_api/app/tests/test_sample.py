def test_get_sample_by_search(client, db_session):
    response = client.get("/api/v1.7/sample/search", params={"keyword": "max"})
    assert response.status_code == 200