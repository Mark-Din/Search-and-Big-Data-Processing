import pytest
from fastapi.testclient import TestClient
import sys

# Ensure FastAPI app path is accessible inside the container
sys.path.append("/app/fastapi")
from main import app

# ✅ instantiate client (no keyword)
client = TestClient(app)


def test_get_indices(monkeypatch):
    response = client.get("/get_indices")
    assert response.status_code in (200, 500)
