import pytest
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch
import sys

# Ensure FastAPI app path is accessible inside the container
sys.path.append("/app/fastapi")
from main import app
from routers.fastAPI_UI_elasticSearch import ElasticSearchConnectionManager

# ✅ instantiate client (no keyword)
client = TestClient(app)

@patch("routers.fastAPI_UI_elasticSearch.ElasticSearchConnectionManager.get_instance")
def test_get_indices(mock_get_instance):

    mock_es = MagicMock()
    mock_get_instance.return_value = mock_es

    mock_es.indices.get_alias.return_value = [
        "arxiv_clusters"
        # "wholecorp_clusters_vector":{}
    ]

    response = client.get(f"/get_indices")

    print("response.json():", response.json())
    assert response.status_code in (200, 500)
    assert "arxiv_clusters" in response.json() #and "wholecorp_clusters_vector" in response.json()

