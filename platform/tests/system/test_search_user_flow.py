# import pytest
# from fastapi.testclient import TestClient
# from elasticsearch import Elasticsearch
# from unittest.mock import MagicMock
# import sys, os

# # import FastAPI app
# PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
# sys.path.insert(0, os.path.join(PROJECT_ROOT, "search_system", "FastAPI_service", "code", "fastapi"))

# from main import app
# from routers.fastAPI_UI_elasticSearch import ElasticSearchConnectionManager

# client = TestClient(app)

# @pytest.mark.system
# def test_user_search_flow():
#     # 1. Call search API

#     mock_es = MagicMock()
#     mock_es.indices.get_alias.return_value = {"test-index": {}}

#     app.dependency_overrides[ElasticSearchConnectionManager.get_instance] = lambda: mock_es
#     res = client.post("/full_search?query=company")

#     assert res.status_code in (200, 500)
    
#     # It needs a parameter
#     res = client.post("/full_search?")
#     assert res.status_code == 422

#     # 2. If ES online, verify response structure
#     if res.status_code == 200 and isinstance(res.json(), dict):
#         assert "results" in res.json()
