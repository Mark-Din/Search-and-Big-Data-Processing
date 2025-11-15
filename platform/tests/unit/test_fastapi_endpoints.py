import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock
import sys, os

# Correct project root
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(PROJECT_ROOT, "search_system", "FastAPI_service", "code", "fastapi"))

from main import app
from routers.fastAPI_UI_elasticSearch import ElasticSearchConnectionManager


@pytest.mark.unit
def test_get_indices_smoke():
    # Create a mock ES client
    mock_es = MagicMock()
    mock_es.indices.get_alias.return_value = {"test-index": {}}

    # Override FastAPI dependency
    app.dependency_overrides[ElasticSearchConnectionManager.get_instance] = lambda: mock_es

    client = TestClient(app)

    response = client.get("/get_indices")

    assert response.status_code == 200
    assert response.json() == ["test-index"]
