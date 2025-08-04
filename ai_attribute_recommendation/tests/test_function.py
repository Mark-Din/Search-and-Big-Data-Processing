from httpx import AsyncClient
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import ASGITransport
from main import app
import pytest

async def test_ai_recommendation():
    async with AsyncClient(app=app, base_url="http://test") as client:
        response = await client.get("/api/seg_recommend_ai")
        assert response.status_code == 200
        data = response.json()
        assert "recommendations" in data
        assert isinstance(data["recommendations"], list)