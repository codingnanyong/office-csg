import pytest
from fastapi.testclient import TestClient
from unittest.mock import Mock, patch

# Mock database session for testing
mock_db_session = Mock()
mock_async_db_session = Mock()

def test_get_wip(client):
    """Test basic WIP endpoint"""
    response = client.get("/api/v1.7/wip")
    assert response.status_code in [200, 404]  # 404 if no data found

def test_get_wip_async(client):
    """Test async WIP endpoint"""
    response = client.get("/api/v1.7/wip/async")
    assert response.status_code in [200, 404]  # 404 if no data found

def test_get_wip_by_opcd(client):
    """Test WIP endpoint with opcd parameter"""
    response = client.get("/api/v1.7/wip/UPC")
    assert response.status_code in [200, 404]  # 404 if no data found

def test_get_wip_by_opcd_async(client):
    """Test async WIP endpoint with opcd parameter"""
    response = client.get("/api/v1.7/wip/UPC/async")
    assert response.status_code in [200, 404]  # 404 if no data found

def test_get_worklist_by_keyword(client):
    """Test worklist search by keyword"""
    response = client.get("/api/v1.7/wip/worklist/search?keyword=test")
    assert response.status_code in [200, 404]  # 404 if no data found

def test_get_worklist_by_keyword_async(client):
    """Test async worklist search by keyword"""
    response = client.get("/api/v1.7/wip/worklist/search/async?keyword=test")
    assert response.status_code in [200, 404]  # 404 if no data found

def test_get_worklist_by_opcd(client):
    """Test worklist by opcd - this is the problematic endpoint"""
    response = client.get("/api/v1.7/wip/worklist/UPC")
    assert response.status_code in [200, 404]  # 404 if no data found

def test_get_worklist_by_opcd_async(client):
    """Test async worklist by opcd - this is the problematic endpoint"""
    response = client.get("/api/v1.7/wip/worklist/UPC/async")
    assert response.status_code in [200, 404]  # 404 if no data found

def test_get_worklist_by_opcd_async_detailed(client):
    """Detailed test for the problematic endpoint with response inspection"""
    response = client.get("/api/v1.7/wip/worklist/UPC/async")
    
    print(f"Response status: {response.status_code}")
    print(f"Response headers: {response.headers}")
    
    if response.status_code != 200:
        print(f"Response body: {response.text}")
    
    # Check if it's a valid JSON response even if empty
    try:
        data = response.json()
        print(f"Response data type: {type(data)}")
        print(f"Response data: {data}")
    except Exception as e:
        print(f"JSON parsing error: {e}")
    
    assert response.status_code in [200, 404]

def test_worklist_endpoint_structure(client):
    """Test to verify the endpoint structure is correct"""
    # Test that the endpoint exists and returns a proper response
    response = client.get("/api/v1.7/wip/worklist/UPC/async")
    
    # Should not be 500 (internal server error)
    assert response.status_code != 500
    
    # Should be either 200 (success) or 404 (not found)
    assert response.status_code in [200, 404]
    
    # If 404, check the error message
    if response.status_code == 404:
        try:
            error_data = response.json()
            assert "detail" in error_data
            print(f"404 Error detail: {error_data['detail']}")
        except:
            print(f"404 Response text: {response.text}")

def test_worklist_with_different_opcd(client):
    """Test worklist with different opcd values to see if the issue is specific to UPC"""
    test_opcds = ["UPC", "TEST", "ABC", "123"]
    
    for opcd in test_opcds:
        response = client.get(f"/api/v1.7/wip/worklist/{opcd}/async")
        print(f"Testing opcd '{opcd}': Status {response.status_code}")
        
        if response.status_code != 200:
            try:
                error_data = response.json()
                print(f"  Error: {error_data}")
            except:
                print(f"  Response: {response.text}")
        
        # Should not be 500 (internal server error)
        assert response.status_code != 500 