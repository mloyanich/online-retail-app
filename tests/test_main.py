from fastapi.testclient import TestClient

from clippie.main import app

client = TestClient(app)


def test_product_search_empty():
    response = client.get("/product?search=\"masha\"")
    assert response.status_code == 200
    assert response.json() == []

def test_product_search_found():
    response = client.get("/product?search=\"coat\"")
    assert response.status_code == 200
    assert len(response.json()) == 9