from http import HTTPStatus

from fastapi.testclient import TestClient

from run import app


def test_routes():
    client = TestClient(app)
    response = client.get("/")
    assert response.status_code == HTTPStatus.OK
    assert response.json()["error"] == ""
