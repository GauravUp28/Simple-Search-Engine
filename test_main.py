import pytest
from fastapi.testclient import TestClient
from main import app, DATA_STORE, INVERTED_INDEX, ingest_data, tokenize
from unittest.mock import patch, MagicMock

client = TestClient(app)

@pytest.fixture(autouse=True)
def run_before_and_after_tests():
    DATA_STORE.clear()
    INVERTED_INDEX.clear()
    yield
    DATA_STORE.clear()
    INVERTED_INDEX.clear()

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "records_indexed": 0}

def test_tokenize():
    text = "Hello, World! This is a TEST."
    tokens = tokenize(text)
    expected = ["hello", "world", "this", "is", "a", "test"]
    assert set(tokens) == set(expected)

def test_search_basic():
    DATA_STORE.append({"message": "I need a reservation for dinner"})
    DATA_STORE.append({"message": "The weather is nice"})
    
    INVERTED_INDEX["reservation"].append(0)
    INVERTED_INDEX["dinner"].append(0)
    INVERTED_INDEX["weather"].append(1)

    response = client.get("/search?q=reservation")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert data["results"][0]["message"] == "I need a reservation for dinner"

def test_search_case_insensitive():
    DATA_STORE.append({"message": "I need a reservation"})
    INVERTED_INDEX["reservation"].append(0)

    response = client.get("/search?q=RESERVATION")
    assert response.status_code == 200
    data = response.json()
    assert data
    assert data["count"] == 1

def test_search_no_results():
    DATA_STORE.append({"message": "Hello world"})
    INVERTED_INDEX["hello"].append(0)

    response = client.get("/search?q=banana")
    assert response.status_code == 200
    assert response.json()["count"] == 0
    assert response.json()["results"] == []

def test_search_pagination():
    for i in range(20):
        DATA_STORE.append({"message": f"Record {i}"})
        INVERTED_INDEX["record"].append(i)

    response = client.get("/search?q=record&limit=10&offset=0")
    data = response.json()
    assert len(data["results"]) == 10
    assert data["results"][0]["message"] == "Record 0"

    response = client.get("/search?q=record&limit=5&offset=10")
    data = response.json()
    assert len(data["results"]) == 5
    assert data["results"][0]["message"] == "Record 10"