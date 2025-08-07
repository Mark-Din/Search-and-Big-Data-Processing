import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
from fastapi import status
from connection import ElasticSearchConnectionManager
from main import app


client = TestClient(app)

def test_read_main():
    response = client.get("/healthy")
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"status": "healthy"}


def mock_es():
    """Create a mock ES client with mocked methods."""
    es_mock = MagicMock()
    es_mock.indices.analyze.return_value = {"tokens": [{"token": "test"}]}
    es_mock.search.return_value = {
        "hits": {
            "total": {"value": 1},
            "hits": [
                {
                    "_source": {
                        "createdAt": "2024-03-22T00:00:00",
                        "updatedAt": "2024-03-22T00:00:00",
                        "price": 1200,
                        "title": "Sample"
                    },
                    "_score": 1.5
                }
            ]
        }
    }
    return es_mock

def test_init_success(monkeypatch):
    monkeypatch.setattr(
        ElasticSearchConnectionManager, "get_instance", lambda: None
    )
    assert client.get("/search/init").status_code == 200


def test_search_suggestions():
    # 1️⃣ inject mock
    app.dependency_overrides[ElasticSearchConnectionManager.get_instance] = mock_es

    response = client.get("/search/search_suggestions", params={"query": "環保"})
    assert response.status_code == 200
    body = response.json()
    for key in ("articleallgets", "useresg", "lifecircleesg", "chatesg"):
        assert key in body

    # 2️⃣ clean up so other tests aren’t affected
    app.dependency_overrides.clear()


def test_search_specific():
    app.dependency_overrides[ElasticSearchConnectionManager.get_instance] = mock_es

    resp = client.get(
        "/search/search_specific",
        params={"unique_id": "123", "ResultsType": "lifecircleesg"},
    )
    assert resp.status_code == 200
    assert "results" in resp.json()

    app.dependency_overrides.clear()